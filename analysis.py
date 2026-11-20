"""Analysis queries over the finished Delta Lake tables.

Six read-only queries a data engineer would run against the lakehouse,
answered straight from the Delta tables (no re-derivation from raw CSVs).
Prints results and saves them to outputs/analysis.json.
"""

import json
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

ROOT = Path(__file__).parent
WH = ROOT / "warehouse"
OUT = ROOT / "outputs"


def money(x):
    return round(float(x), 2)


def main():
    silver = DeltaTable(str(WH / "silver" / "orders_silver")).to_pandas()
    paid = silver[silver["status"] == "paid"]
    results = {}

    # Q1: monthly paid-revenue trend
    monthly = (
        paid.assign(month=paid["order_date"].str[:7])
        .groupby("month")["amount"].sum().round(2)
    )
    results["monthly_paid_revenue"] = {k: money(v) for k, v in monthly.items()}
    results["mom_growth_pct"] = {
        m: round((monthly[m] / monthly[prev] - 1) * 100, 2)
        for m, prev in zip(monthly.index[1:], monthly.index[:-1])
    }

    # Q2: revenue by plan
    by_plan = paid.groupby("plan")["amount"].sum().round(2).sort_values(ascending=False)
    results["paid_revenue_by_plan"] = {k: money(v) for k, v in by_plan.items()}

    # Q3: average order value by channel
    results["avg_order_value_by_channel"] = {
        k: money(v)
        for k, v in paid.groupby("channel")["amount"].mean().round(2).items()
    }

    # Q4: repeat-purchase rate by channel (share of customers with 2+ paid orders)
    repeat_rate = {}
    for ch, grp in paid.groupby("channel"):
        counts = grp.groupby("customer_id").size()
        repeat_rate[ch] = round(float((counts >= 2).mean() * 100), 2)
    results["repeat_customer_rate_pct_by_channel"] = repeat_rate

    # Q5: top 5 customers by paid revenue
    top5 = paid.groupby("customer_id")["amount"].sum().round(2).nlargest(5)
    results["top_5_customers"] = {k: money(v) for k, v in top5.items()}

    # Q6: data-quality recap from the silver table itself
    results["silver_row_count"] = len(silver)
    results["duplicate_order_ids"] = int(len(silver) - silver["order_id"].nunique())
    results["status_breakdown"] = silver["status"].value_counts().to_dict()
    results["null_channels"] = int(silver["channel"].isna().sum())

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "analysis.json", "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
