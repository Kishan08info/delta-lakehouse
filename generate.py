"""Generate seeded synthetic SaaS order data for the Delta Lake lakehouse demo.

All data is SYNTHETIC (seed 42) — a fictional SaaS company, "Northwind
Analytics". Nothing here is real customer data.

Outputs (data/raw/):
  customers.csv        300 customers
  orders_batch1.csv    Jan-Mar 2026 orders (1,090 rows)
  orders_batch2.csv    Apr-Jun 2026 orders (1,274 unique + 12 test orders
                       + 24 duplicate deliveries = 1,310 rows)
  orders_late.csv      36 late-arriving June orders
  corrections.csv      3 rows: amounts recorded 10x too high in batch 1,
                       with the correct amount (fixed later via Delta MERGE)

Deliberate defects (the pipeline must catch them, not hide them):
  - 3 amounts recorded 10x too high (all March orders)
  - 24 duplicate deliveries of batch-2 orders (same order_id, same payload)
  - 14 invalid rows: 9 negative amounts, 5 unknown status 'mystery'
  - 12 test orders (status 'test') to be removed with a Delta DELETE
"""

import csv
import random
from pathlib import Path

SEED = 42
RAW = Path(__file__).parent / "data" / "raw"

PLANS = ["starter", "growth", "pro", "enterprise"]
PLAN_W = [0.40, 0.30, 0.20, 0.10]
PLAN_BASE = {"starter": 60.0, "growth": 220.0, "pro": 650.0, "enterprise": 2400.0}
CHANNELS = ["organic", "paid_search", "referral", "social"]
CHANNEL_W = [0.35, 0.30, 0.20, 0.15]
STATUSES = ["paid", "refunded", "failed"]
STATUS_W = [0.82, 0.08, 0.10]


def rand_date(rng, year, month):
    day = rng.randint(1, 28)  # safe for every month
    return f"{year}-{month:02d}-{day:02d}"


def make_customers(rng, n=300):
    rows = []
    for i in range(1, n + 1):
        plan = rng.choices(PLANS, weights=PLAN_W)[0]
        rows.append(
            {
                "customer_id": f"C{i:04d}",
                "plan": plan,
                "channel": rng.choices(CHANNELS, weights=CHANNEL_W)[0],
                "signup_date": rand_date(rng, 2026, rng.randint(1, 6)),
            }
        )
    return rows


def make_order(rng, order_id, customers, month):
    cust = rng.choice(customers)
    base = PLAN_BASE[cust["plan"]]
    amount = round(rng.uniform(0.5, 1.8) * base, 2)
    return {
        "order_id": order_id,
        "customer_id": cust["customer_id"],
        "plan": cust["plan"],
        "order_date": rand_date(rng, 2026, month),
        "amount": amount,
        "status": rng.choices(STATUSES, weights=STATUS_W)[0],
    }


def main():
    rng = random.Random(SEED)
    RAW.mkdir(parents=True, exist_ok=True)
    customers = make_customers(rng)
    with open(RAW / "customers.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["customer_id", "plan", "channel", "signup_date"])
        w.writeheader()
        w.writerows(customers)

    orders = []
    oid = 1

    def next_id():
        nonlocal oid
        oid += 1
        return f"O{oid - 1:06d}"

    # Batch 1: Jan-Mar, 1,090 orders
    batch1 = []
    for _ in range(1090):
        batch1.append(make_order(rng, next_id(), customers, rng.randint(1, 3)))

    # 3 amounts recorded 10x too high (all March) + 5 negative + 3 mystery status
    march_idx = [i for i, o in enumerate(batch1) if o["order_date"].startswith("2026-03")]
    bad_idx = rng.sample(march_idx, 3)
    corrections = []
    for i in bad_idx:
        correct = batch1[i]["amount"]
        batch1[i]["amount"] = round(correct * 10, 2)
        corrections.append({"order_id": batch1[i]["order_id"], "correct_amount": correct})
    for i in rng.sample(range(len(batch1)), 5):
        batch1[i]["amount"] = -abs(batch1[i]["amount"])
    for i in rng.sample(range(len(batch1)), 3):
        batch1[i]["status"] = "mystery"

    # Batch 2: Apr-Jun, 1,274 unique orders
    batch2 = []
    for _ in range(1274):
        batch2.append(make_order(rng, next_id(), customers, rng.randint(4, 6)))
    for i in rng.sample(range(len(batch2)), 4):
        batch2[i]["amount"] = -abs(batch2[i]["amount"])
    for i in rng.sample(range(len(batch2)), 2):
        batch2[i]["status"] = "mystery"
    # 12 test orders (status 'test') — to be removed later with a Delta DELETE
    for _ in range(12):
        o = make_order(rng, next_id(), customers, rng.randint(4, 6))
        o["status"] = "test"
        batch2.append(o)
    # 24 duplicate deliveries (exact copies of existing batch-2 orders)
    dups = [dict(o) for o in rng.sample(batch2[:1274], 24)]
    batch2_rows = batch2 + dups

    # Late batch: 36 June orders delivered after batch 2
    late = [make_order(rng, next_id(), customers, 6) for _ in range(36)]

    fields = ["order_id", "customer_id", "plan", "order_date", "amount", "status"]
    for name, rows in [
        ("orders_batch1.csv", batch1),
        ("orders_batch2.csv", batch2_rows),
        ("orders_late.csv", late),
    ]:
        with open(RAW / name, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
    with open(RAW / "corrections.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["order_id", "correct_amount"])
        w.writeheader()
        w.writerows(corrections)

    print(f"customers: {len(customers)}")
    print(f"batch1 rows: {len(batch1)} (incl. 3 bad amounts, 5 negative, 3 mystery)")
    print(f"batch2 rows: {len(batch2_rows)} (1,274 unique + 12 test + 24 dup deliveries)")
    print(f"late rows: {len(late)}")
    print(f"corrections: {len(corrections)}")
    print(f"total unique orders: {1090 + 1274 + 36 + 12}")
    print(f"total delivered rows: {len(batch1) + len(batch2_rows) + len(late)}")


if __name__ == "__main__":
    main()
