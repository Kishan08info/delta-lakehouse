# Lakehouse with Delta Lake

**SYNTHETIC DATA** — every number below comes from a seeded, re-runnable demo
run over fictional SaaS data ("Northwind Analytics"). This is a portfolio
skill demo, **not** work experience.

A bronze → silver → gold lakehouse built on **real Delta Lake** (the
`deltalake` / delta-rs library, v1.6.6) — not a simulation of it. Every
operation below is a genuine Delta transaction with a version in the
transaction log: ACID appends, `MERGE` upserts, row-level `DELETE`, schema
evolution via `ALTER TABLE`, time travel, `OPTIMIZE` + Z-ORDER, `VACUUM`,
and change-data-feed reads.

## Why this exists

Data Engineer postings keep asking for a **lakehouse** — Kin Insurance
("build out a data lakehouse for enterprise reporting"), Definitive
Healthcare (S3 data lake), Superhuman (**Delta Lake** core). Project 23
covered the other major open table format (Apache Iceberg); this one covers
Delta Lake, so the two can be compared honestly.

## Business question

> A SaaS company lands order events from its billing system into a lakehouse.
> Can we fix bad amounts after the fact, add a column without rewriting the
> table, prove what changed, and keep the warehouse fast and clean — using
> only the table format's own guarantees?

## What the pipeline does

| Stage | Delta operation | Result |
|---|---|---|
| Bronze `orders_bronze` | ACID append, partitioned by `order_month`, change-data-feed on | 2,436 delivered rows (2,412 unique order IDs) |
| Silver load | 3 incremental appends (Jan–Mar, Apr–Jun, late June) | 2,398 rows after dedupe + quarantine |
| Dedupe | `drop_duplicates` on `order_id`, keep latest delivery | 24 duplicate deliveries removed |
| Quarantine | invalid rows to `orders_quarantine` with a reason | 14 rows: 9 negative amounts, 5 unknown statuses |
| **MERGE** | fix 3 amounts recorded 10× too high | March paid revenue $145,654.71 → **$143,042.64** (−$2,612.07) |
| **OPTIMIZE** | compact + Z-ORDER by `customer_id` | 3 files → **1 file** |
| **Schema evolution** | `ALTER TABLE … ADD COLUMN channel`, then MERGE backfill from customers | 0 NULL channels, no table rewrite |
| **DELETE** | `status = 'test'` | 12 test orders removed → **2,386 rows** |
| Gold marts | `mart_monthly_revenue`, `mart_revenue_by_plan`, `mart_top_customers` | monthly revenue reconciles to the cent |
| **VACUUM** | dry-run, then remove stale files (retention 0) | 7 files removed; current paid revenue unchanged ($1,092,574.98) |

## Results (seed 42, verified by a from-scratch re-run — byte-identical)

- **Final silver:** 2,386 orders — 1,989 paid / 181 refunded / 216 failed, 0 duplicate order IDs
- **Paid revenue:** **$1,092,574.98**; refunded $105,747.94; **net $986,827.04**
- **Monthly paid revenue:** Jan $177,584.53 · Feb $179,983.17 · Mar $143,042.64 · Apr $211,516.98 · May $154,280.64 · Jun $226,167.02
- **Revenue by plan:** enterprise $596,374.59 (54.58%) · pro $273,886.77 · growth $172,080.43 · starter $50,233.19
- **Top customer:** C0079, $35,361.20
- **Time travel:** version 2 (before MERGE) shows March at $145,654.71; version 3 (after MERGE) shows $143,042.64 — the fix is provable from history alone
- **Change data feed:** the MERGE commit (v3) yields exactly 6 rows — 3 `update_preimage` + 3 `update_postimage` for O000072, O000187, O000276
- **Reconciliation:** an independent pandas recomputation from the raw CSVs matches silver exactly — $1,092,574.98 and 2,386 rows

## Delta Lake vs Apache Iceberg (from building both — see Project 23)

| Feature | Delta Lake (this project) | Iceberg (Project 23) |
|---|---|---|
| Time travel | version / timestamp, same idea | snapshot IDs, same idea |
| Upsert | `MERGE` with per-column `SET` | row-level overwrite via `upsert` |
| Schema evolution | `ALTER TABLE ADD COLUMN` + MERGE backfill | `UpdateSpec.add_field`, backfill via overwrite |
| File layout | `OPTIMIZE` + Z-ORDER | compaction via rewrite; partition evolution in place |
| Change feed | built-in CDF (`delta.enableChangeDataFeed`) | no built-in CDF in PyIceberg 0.12 |
| Deletes | `DELETE` with predicate | row-level delete via `delete` |
| Gotcha found | `DeltaTable` objects cache their snapshot — re-load after `ALTER` | upsert can't merge into files written before a schema evolution |

## Honest labelling

- **Local files stand in for S3.** The warehouse is a local directory; in
  production the same code points `write_deltalake` at `s3://…`. No cloud
  account, no cluster, no Spark — Delta Lake runs here in pure Python.
- **Synthetic data everywhere.** Seeded (`seed = 42`), 300 customers, 2,412
  unique orders, Jan–Jun 2026. Month-to-month swings are random variation in
  the synthetic data, not a business trend.
- **Known limitation:** `OPTIMIZE` runs before the schema-evolution step —
  a later full-table overwrite would rewrite files again, so in production
  you re-run `OPTIMIZE` after bulk rewrites. The 99–100% repeat-purchase
  rates in `analysis.json` are an artifact of 300 customers sharing ~2,000
  orders, not a real retention signal.

## How to run

```bash
pip install -r requirements.txt   # deltalake, pyarrow, pandas, pytest
python run.py --fresh             # generate data + run pipeline + analysis
python -m pytest tests/ -q        # 15 checks, all against the verified run
```

Outputs: `outputs/summary.json` (every number above), `outputs/analysis.json`
(6 BI queries), `outputs/mart_*.csv`. The warehouse itself (`warehouse/`)
is git-ignored build output — regenerate it with `run.py`.

## Repo layout

```
generate.py    seeded synthetic data -> data/raw/*.csv
pipeline.py    bronze -> silver -> gold, all Delta operations
analysis.py    6 read-only queries over the finished Delta tables
run.py         generate + pipeline + analysis, end to end
tests/         15 pytest checks against the verified seed-42 numbers
data/raw/      generated CSVs (small, committed so the repo is self-contained)
outputs/       summary.json, analysis.json, mart CSVs from the verified run
```
