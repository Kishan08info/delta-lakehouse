"""Delta Lake lakehouse pipeline — bronze -> silver -> gold.

Real Delta Lake operations via the `deltalake` library (delta-rs), on local
files that stand in for an S3-backed lakehouse. No Delta feature here is
simulated: appends, MERGE upserts, DELETE, schema evolution, time travel,
OPTIMIZE + Z-ORDER, VACUUM and change-data-feed reads are all real Delta
Lake transactions, each one a new table version in the transaction log.

Stages:
  1. bronze  orders_bronze      raw deliveries, partitioned by order_month
  2. silver  orders_silver      deduped + quarantined, MERGE amount fixes,
                                channel column added (schema evolution),
                                test orders removed (DELETE)
  3. gold    mart_monthly_revenue / mart_revenue_by_plan / mart_top_customers
  4. ops     OPTIMIZE (compact + Z-ORDER by customer_id), VACUUM, CDF read

All results are written to outputs/summary.json. Every number in the README
comes from that file, i.e. from an actual run — never hand-typed.
"""

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.schema import Field

ROOT = Path(__file__).parent
RAW = ROOT / "data" / "raw"
WH = ROOT / "warehouse"
OUT = ROOT / "outputs"

BRONZE = str(WH / "bronze" / "orders_bronze")
SILVER = str(WH / "silver" / "orders_silver")
QUARANTINE = str(WH / "silver" / "orders_quarantine")
GOLD = WH / "gold"

VALID_STATUSES = {"paid", "refunded", "failed", "test"}
CDF_CONFIG = {"delta.enableChangeDataFeed": "true"}


def money(x):
    return round(float(x), 2)


def load_raw():
    frames = []
    for name in ["orders_batch1.csv", "orders_batch2.csv", "orders_late.csv"]:
        frames.append(pd.read_csv(RAW / name))
    raw = pd.concat(frames, ignore_index=True)
    raw["_delivery_seq"] = range(1, len(raw) + 1)
    raw["order_month"] = raw["order_date"].str[:7]
    return raw


def march_paid_revenue(df):
    m = df[df["order_date"].str.startswith("2026-03") & (df["status"] == "paid")]
    return money(m["amount"].sum())


def main():
    WH.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    (GOLD).mkdir(parents=True, exist_ok=True)
    summary = {}

    # ---------------------------------------------------------------- bronze
    raw = load_raw()
    summary["raw_delivered_rows"] = len(raw)
    summary["raw_unique_order_ids"] = int(raw["order_id"].nunique())
    write_deltalake(
        BRONZE, raw, mode="overwrite", partition_by=["order_month"],
        configuration=CDF_CONFIG,
    )
    bronze = DeltaTable(BRONZE)
    summary["bronze_version"] = bronze.version()
    summary["bronze_rows"] = len(bronze.to_pandas())
    print(f"bronze: {summary['bronze_rows']} rows, version {bronze.version()}")

    # ---------------------------------------------------------------- silver
    bdf = bronze.to_pandas().sort_values("_delivery_seq")
    deduped = bdf.drop_duplicates(subset=["order_id"], keep="last")
    summary["duplicate_deliveries_removed"] = int(len(bdf) - len(deduped))

    def quarantine_reason(row):
        if row["amount"] <= 0:
            return "negative_amount"
        if row["status"] not in VALID_STATUSES:
            return "unknown_status"
        return None

    deduped = deduped.copy()
    deduped["quarantine_reason"] = deduped.apply(quarantine_reason, axis=1)
    quarantined = deduped[deduped["quarantine_reason"].notna()].copy().reset_index(drop=True)
    silver_df = deduped[deduped["quarantine_reason"].isna()].drop(
        columns=["quarantine_reason", "order_month"]
    ).reset_index(drop=True)
    summary["quarantined_rows"] = int(len(quarantined))
    summary["quarantine_by_reason"] = (
        quarantined["quarantine_reason"].value_counts().to_dict()
    )
    write_deltalake(
        QUARANTINE, quarantined, mode="overwrite", configuration=CDF_CONFIG
    )
    # Load silver incrementally (3 appends: Jan-Mar, Apr-Jun, late June),
    # the way a real pipeline lands batches — so OPTIMIZE has files to combine.
    chunks = [
        silver_df[silver_df["_delivery_seq"] <= 1090],
        silver_df[(silver_df["_delivery_seq"] > 1090) & (silver_df["_delivery_seq"] <= 2400)],
        silver_df[silver_df["_delivery_seq"] > 2400],
    ]
    for i, chunk in enumerate(chunks):
        write_deltalake(
            SILVER, chunk, mode="overwrite" if i == 0 else "append",
            configuration=CDF_CONFIG,
        )
    silver = DeltaTable(SILVER)
    v_silver0 = silver.version()
    summary["silver_version_after_load"] = v_silver0
    summary["silver_rows_after_load"] = len(silver.to_pandas())
    summary["silver_load_chunks"] = [int(len(c)) for c in chunks]
    print(f"silver v{v_silver0}: {summary['silver_rows_after_load']} rows in "
          f"{len(chunks)} appends {summary['silver_load_chunks']}, "
          f"{summary['quarantined_rows']} quarantined")

    # ------------------------------------------------- MERGE: fix 10x amounts
    summary["march_paid_revenue_before_fix"] = march_paid_revenue(
        DeltaTable(SILVER, version=v_silver0).to_pandas()
    )
    corrections = pd.read_csv(RAW / "corrections.csv").rename(
        columns={"correct_amount": "amount"}
    )
    src = pa.Table.from_pandas(corrections[["order_id", "amount"]])
    (DeltaTable(SILVER)
     .merge(source=src, predicate="t.order_id = s.order_id",
            source_alias="s", target_alias="t")
     .when_matched_update({"amount": "s.amount"})
     .execute())
    silver = DeltaTable(SILVER)
    v_merge = silver.version()
    summary["silver_version_after_merge"] = v_merge
    summary["march_paid_revenue_after_fix"] = march_paid_revenue(silver.to_pandas())
    summary["march_overstatement_fixed"] = money(
        summary["march_paid_revenue_before_fix"]
        - summary["march_paid_revenue_after_fix"]
    )
    print(f"MERGE v{v_merge}: March paid revenue "
          f"${summary['march_paid_revenue_before_fix']:,.2f} -> "
          f"${summary['march_paid_revenue_after_fix']:,.2f}")

    # --------------------------------------- OPTIMIZE (before schema change:
    # a later full overwrite would rewrite the files again, so optimize now)
    silver = DeltaTable(SILVER)
    summary["optimize"] = {"files_before": len(silver.file_uris())}
    silver.optimize.compact()
    summary["optimize"]["files_after_compact"] = len(DeltaTable(SILVER).file_uris())
    DeltaTable(SILVER).optimize.z_order(["customer_id"])
    summary["optimize"]["files_after_z_order"] = len(DeltaTable(SILVER).file_uris())
    summary["optimize"]["version"] = DeltaTable(SILVER).version()
    print(f"OPTIMIZE: {summary['optimize']}")

    # --------------------------------------- schema evolution: add `channel`
    # ALTER TABLE adds the column (NULL for existing rows); a MERGE from the
    # customers dimension backfills it — no full-table overwrite needed.
    silver = DeltaTable(SILVER)
    silver.alter.add_columns([Field("channel", "string")])
    silver = DeltaTable(SILVER)  # alter bumps the version; re-load the snapshot
    v_schema = silver.version()
    customers = pd.read_csv(RAW / "customers.csv")[["customer_id", "channel"]]
    chan_src = pa.Table.from_pandas(
        silver.to_pandas()
        .drop(columns=["channel"])  # ALTER added it as all-NULL; customers has the values
        .merge(customers, on="customer_id", how="left")[["order_id", "channel"]]
    )
    (DeltaTable(SILVER)
     .merge(source=chan_src, predicate="t.order_id = s.order_id",
            source_alias="s", target_alias="t")
     .when_matched_update({"channel": "s.channel"})
     .execute())
    silver = DeltaTable(SILVER)
    v_backfill = silver.version()
    summary["silver_version_after_schema_evolution"] = v_schema
    summary["silver_version_after_channel_backfill"] = v_backfill
    summary["silver_columns"] = list(silver.to_pandas().columns)
    summary["channel_nulls_after_backfill"] = int(silver.to_pandas()["channel"].isna().sum())
    print(f"schema evolution v{v_schema} + backfill v{v_backfill}: columns now "
          f"{summary['silver_columns']}, null channels: "
          f"{summary['channel_nulls_after_backfill']}")

    # ------------------------------------------------- DELETE: test orders
    before_delete = len(silver.to_pandas())
    DeltaTable(SILVER).delete(predicate="status = 'test'")
    silver = DeltaTable(SILVER)
    v_delete = silver.version()
    after_delete = len(silver.to_pandas())
    summary["silver_version_after_delete"] = v_delete
    summary["test_orders_deleted"] = int(before_delete - after_delete)
    summary["silver_rows_final"] = after_delete
    print(f"DELETE v{v_delete}: {summary['test_orders_deleted']} test orders removed, "
          f"{after_delete} rows remain")

    # ------------------------------------------------------------------ gold
    final = silver.to_pandas()
    summary["final_paid_orders"] = int((final["status"] == "paid").sum())
    summary["final_paid_revenue"] = money(
        final.loc[final["status"] == "paid", "amount"].sum()
    )
    summary["final_refunded_amount"] = money(
        final.loc[final["status"] == "refunded", "amount"].sum()
    )
    summary["final_net_revenue"] = money(
        summary["final_paid_revenue"] - summary["final_refunded_amount"]
    )

    monthly = (
        final.assign(order_month=final["order_date"].str[:7])
        .groupby("order_month")
        .apply(lambda g: pd.Series({
            "orders": len(g),
            "paid_orders": int((g["status"] == "paid").sum()),
            "paid_revenue": money(g.loc[g["status"] == "paid", "amount"].sum()),
            "refunded_amount": money(g.loc[g["status"] == "refunded", "amount"].sum()),
        }), include_groups=False)
        .reset_index()
    )
    monthly["net_revenue"] = (monthly["paid_revenue"] - monthly["refunded_amount"]).round(2)
    by_plan = (
        final.groupby("plan")
        .apply(lambda g: pd.Series({
            "orders": len(g),
            "paid_orders": int((g["status"] == "paid").sum()),
            "paid_revenue": money(g.loc[g["status"] == "paid", "amount"].sum()),
        }), include_groups=False)
        .reset_index()
    )
    by_plan["revenue_share_pct"] = (
        by_plan["paid_revenue"] / summary["final_paid_revenue"] * 100
    ).round(2)
    top_customers = (
        final[final["status"] == "paid"]
        .groupby("customer_id")["amount"].sum().round(2)
        .sort_values(ascending=False).head(10).reset_index()
        .rename(columns={"amount": "paid_revenue"})
    )
    for name, df in [
        ("mart_monthly_revenue", monthly),
        ("mart_revenue_by_plan", by_plan),
        ("mart_top_customers", top_customers),
    ]:
        write_deltalake(str(GOLD / name), df, mode="overwrite")
    summary["mart_monthly_rows"] = len(monthly)
    summary["mart_monthly_revenue_total"] = money(monthly["paid_revenue"].sum())
    summary["mart_revenue_by_plan_total"] = money(by_plan["paid_revenue"].sum())
    summary["top_customer"] = {
        "customer_id": str(top_customers.iloc[0]["customer_id"]),
        "paid_revenue": money(top_customers.iloc[0]["paid_revenue"]),
    }
    print(f"gold: monthly {len(monthly)} rows, by-plan {len(by_plan)} rows, "
          f"top customer {summary['top_customer']}")

    # --------------------------------------- time travel (BEFORE vacuum!)
    v0_march = march_paid_revenue(DeltaTable(SILVER, version=v_silver0).to_pandas())
    vmerge_march = march_paid_revenue(
        DeltaTable(SILVER, version=v_merge).to_pandas()
    )
    summary["time_travel"] = {
        "version_before_merge": v_silver0,
        "march_paid_revenue_before_merge": v0_march,
        "version_after_merge": v_merge,
        "march_paid_revenue_after_merge": vmerge_march,
    }

    # ------------------------------------------------- change data feed (MERGE)
    cdf = pa.table(DeltaTable(SILVER).load_cdf(
        starting_version=v_merge, ending_version=v_merge
    ).read_all()).to_pylist()
    cdf_counts = {}
    for row in cdf:
        cdf_counts[row["_change_type"]] = cdf_counts.get(row["_change_type"], 0) + 1
    summary["cdf_merge_commit"] = {
        "version": v_merge,
        "rows": len(cdf),
        "by_change_type": cdf_counts,
        "corrected_order_ids": sorted(
            {r["order_id"] for r in cdf if r["_change_type"] == "update_postimage"}
        ),
    }
    print(f"CDF v{v_merge}: {len(cdf)} rows {cdf_counts}")

    # ------------------------------------------------- VACUUM (last!)
    paid_before_vacuum = summary["final_paid_revenue"]
    silver = DeltaTable(SILVER)
    stale = silver.vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=True
    )
    summary["vacuum"] = {"stale_files_dry_run": len(stale)}
    removed = silver.vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False
    )
    summary["vacuum"]["files_removed"] = len(removed)
    paid_after_vacuum = money(
        DeltaTable(SILVER).to_pandas()
        .query("status == 'paid'")["amount"].sum()
    )
    summary["vacuum"]["paid_revenue_before"] = paid_before_vacuum
    summary["vacuum"]["paid_revenue_after"] = paid_after_vacuum
    summary["vacuum"]["current_data_unchanged"] = (
        paid_before_vacuum == paid_after_vacuum
    )
    print(f"VACUUM: {len(stale)} stale files listed, {len(removed)} removed; "
          f"current data unchanged: {summary['vacuum']['current_data_unchanged']}")

    # ------------------------------------------------- reconciliation
    corr = pd.read_csv(RAW / "corrections.csv").set_index("order_id")["correct_amount"]
    check = raw.sort_values("_delivery_seq").drop_duplicates("order_id", keep="last")
    check = check[
        (check["amount"] > 0)
        & (check["status"].isin(["paid", "refunded", "failed"]))
    ].copy()
    check.loc[check["order_id"].isin(corr.index), "amount"] = check.loc[
        check["order_id"].isin(corr.index), "order_id"
    ].map(corr)
    expected_paid = money(
        check.loc[check["status"] == "paid", "amount"].sum()
    )
    summary["reconciliation"] = {
        "independent_paid_revenue": expected_paid,
        "silver_paid_revenue": summary["final_paid_revenue"],
        "rows_expected": int(len(check)),
        "rows_silver": summary["silver_rows_final"],
        "exact_match": (
            expected_paid == summary["final_paid_revenue"]
            and len(check) == summary["silver_rows_final"]
        ),
    }
    print(f"reconciliation exact: {summary['reconciliation']['exact_match']}")

    # ------------------------------------------------- history
    summary["history"] = [
        {"version": h["version"], "operation": h["operation"]}
        for h in DeltaTable(SILVER).history()
    ]

    with open(OUT / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    monthly.to_csv(OUT / "mart_monthly_revenue.csv", index=False)
    by_plan.to_csv(OUT / "mart_revenue_by_plan.csv", index=False)
    print("wrote outputs/summary.json")


if __name__ == "__main__":
    main()
