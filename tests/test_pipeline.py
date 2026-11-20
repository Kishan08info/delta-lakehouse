"""End-to-end tests for the Delta Lake lakehouse.

Runs the real pipeline in a fresh temp copy of the project and asserts the
exact numbers from the verified seed-42 run. If the generator, the pipeline
or the Delta library changes behaviour, these fail — that is the point.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent
PY = sys.executable


@pytest.fixture(scope="module")
def run_dir(tmp_path_factory):
    dest = tmp_path_factory.mktemp("delta-lakehouse")
    for item in PROJECT.iterdir():
        if item.name in {"warehouse", "outputs", "__pycache__", ".pytest_cache"}:
            continue
        if item.is_dir():
            shutil.copytree(item, dest / item.name)
        else:
            shutil.copy2(item, dest / item.name)
    r = subprocess.run([PY, "run.py", "--fresh"], cwd=dest,
                       capture_output=True, text=True)
    assert r.returncode == 0, f"run.py failed:\n{r.stdout}\n{r.stderr}"
    return dest


@pytest.fixture(scope="module")
def summary(run_dir):
    return json.loads((run_dir / "outputs" / "summary.json").read_text())


@pytest.fixture(scope="module")
def analysis(run_dir):
    return json.loads((run_dir / "outputs" / "analysis.json").read_text())


def test_raw_counts(summary):
    assert summary["raw_delivered_rows"] == 2436
    assert summary["raw_unique_order_ids"] == 2412
    assert summary["bronze_rows"] == 2436


def test_dedupe_and_quarantine(summary):
    assert summary["duplicate_deliveries_removed"] == 24
    assert summary["quarantined_rows"] == 14
    assert summary["quarantine_by_reason"] == {
        "negative_amount": 9, "unknown_status": 5}


def test_silver_load(summary):
    assert summary["silver_rows_after_load"] == 2398
    assert summary["silver_load_chunks"] == [1082, 1280, 36]


def test_merge_fixes_march_revenue(summary):
    assert summary["march_paid_revenue_before_fix"] == 145654.71
    assert summary["march_paid_revenue_after_fix"] == 143042.64
    assert summary["march_overstatement_fixed"] == 2612.07


def test_optimize_compacts_files(summary):
    opt = summary["optimize"]
    assert opt["files_before"] == 3
    assert opt["files_after_compact"] == 1
    assert opt["files_after_z_order"] == 1


def test_schema_evolution_backfills_channel(summary):
    assert "channel" in summary["silver_columns"]
    assert summary["channel_nulls_after_backfill"] == 0


def test_delete_removes_test_orders(summary):
    assert summary["test_orders_deleted"] == 12
    assert summary["silver_rows_final"] == 2386


def test_final_revenue(summary):
    assert summary["final_paid_orders"] == 1989
    assert summary["final_paid_revenue"] == 1092574.98
    assert summary["final_refunded_amount"] == 105747.94
    assert summary["final_net_revenue"] == 986827.04


def test_time_travel_matches_merge(summary):
    tt = summary["time_travel"]
    assert tt["march_paid_revenue_before_merge"] == 145654.71
    assert tt["march_paid_revenue_after_merge"] == 143042.64
    assert tt["version_after_merge"] == tt["version_before_merge"] + 1


def test_change_data_feed(summary):
    cdf = summary["cdf_merge_commit"]
    assert cdf["rows"] == 6
    assert cdf["by_change_type"] == {
        "update_preimage": 3, "update_postimage": 3}
    assert cdf["corrected_order_ids"] == ["O000072", "O000187", "O000276"]


def test_vacuum_preserves_current_data(summary):
    assert summary["vacuum"]["files_removed"] == 7
    assert summary["vacuum"]["current_data_unchanged"] is True
    assert (summary["vacuum"]["paid_revenue_before"]
            == summary["vacuum"]["paid_revenue_after"] == 1092574.98)


def test_reconciliation_exact(summary):
    rec = summary["reconciliation"]
    assert rec["exact_match"] is True
    assert rec["independent_paid_revenue"] == 1092574.98
    assert rec["rows_expected"] == rec["rows_silver"] == 2386


def test_marts_reconcile(summary):
    assert summary["mart_monthly_revenue_total"] == 1092574.98
    assert summary["mart_revenue_by_plan_total"] == 1092574.98
    assert summary["mart_monthly_rows"] == 6
    assert summary["top_customer"] == {
        "customer_id": "C0079", "paid_revenue": 35361.2}


def test_history_covers_every_operation(summary):
    ops = [h["operation"] for h in summary["history"]]
    for expected in ["WRITE", "MERGE", "OPTIMIZE", "ADD COLUMN", "DELETE",
                     "VACUUM START", "VACUUM END"]:
        assert expected in ops, f"missing {expected} in {ops}"


def test_analysis(analysis):
    assert analysis["silver_row_count"] == 2386
    assert analysis["duplicate_order_ids"] == 0
    assert analysis["null_channels"] == 0
    assert analysis["status_breakdown"] == {
        "paid": 1989, "failed": 216, "refunded": 181}
    assert analysis["monthly_paid_revenue"]["2026-03"] == 143042.64
    assert analysis["monthly_paid_revenue"]["2026-06"] == 226167.02
    assert analysis["paid_revenue_by_plan"]["enterprise"] == 596374.59
    assert analysis["top_5_customers"]["C0079"] == 35361.2
