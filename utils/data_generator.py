# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Data Generator
# MAGIC Generates realistic retail data for the Bronze ingestion layer.
# MAGIC Run this once to populate `data/sample/` before running ingestion notebooks.

# COMMAND ----------

import csv
import random
import os
from datetime import date, timedelta

random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
OUT_DIR = "/dbfs/FileStore/retail_platform/landing"
START_DATE = date(2023, 1, 1)
END_DATE   = date(2024, 12, 31)
NUM_TRANSACTIONS = 50_000

# COMMAND ----------

os.makedirs(OUT_DIR, exist_ok=True)

# ── Reference data ────────────────────────────────────────────────────────────
REGIONS = ["North", "South", "East", "West"]

STORES = [
    (f"S{i:03d}", f"Store {i}", random.choice(["New York","Los Angeles","Chicago","Houston","Phoenix"]),
     random.choice(REGIONS), "US")
    for i in range(1, 21)
]

CATEGORIES = {
    "Electronics": ["Laptop","Tablet","Phone","Headphones","Camera"],
    "Clothing":    ["T-Shirt","Jeans","Jacket","Shoes","Hat"],
    "Home":        ["Sofa","Table","Chair","Lamp","Rug"],
    "Sports":      ["Bike","Treadmill","Yoga Mat","Weights","Tent"],
    "Food":        ["Coffee","Tea","Snacks","Juice","Water"],
}

PRODUCTS = []
pid = 1
for cat, items in CATEGORIES.items():
    for sub in items:
        PRODUCTS.append((
            f"P{pid:04d}", sub, cat, sub,
            round(random.uniform(5.0, 1500.0), 2)
        ))
        pid += 1

SEGMENTS   = ["Consumer", "Corporate", "Home Office"]
CITIES     = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego"]
CUSTOMERS  = [
    (f"C{i:05d}", f"Customer_{i}", f"customer{i}@email.com",
     random.choice(CITIES), random.choice(SEGMENTS))
    for i in range(1, 5001)
]

# COMMAND ----------

def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return str(start + timedelta(days=random.randint(0, delta)))

# ── Write CSVs ────────────────────────────────────────────────────────────────

def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"Written: {path}  ({len(rows):,} rows)")

write_csv(
    f"{OUT_DIR}/stores.csv",
    ["store_id", "store_name", "city", "region", "country"],
    STORES,
)

write_csv(
    f"{OUT_DIR}/products.csv",
    ["product_id", "product_name", "category", "subcategory", "unit_price"],
    PRODUCTS,
)

write_csv(
    f"{OUT_DIR}/customers.csv",
    ["customer_id", "customer_name", "email", "city", "segment"],
    CUSTOMERS,
)

transactions = []
for i in range(1, NUM_TRANSACTIONS + 1):
    prod     = random.choice(PRODUCTS)
    cust     = random.choice(CUSTOMERS)
    store    = random.choice(STORES)
    qty      = random.randint(1, 10)
    discount = round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2)
    amount   = round(prod[4] * qty * (1 - discount), 2)
    status   = random.choices(
        ["completed", "returned", "pending"],
        weights=[85, 10, 5]
    )[0]
    transactions.append((
        f"T{i:07d}", cust[0], prod[0], store[0],
        random_date(START_DATE, END_DATE),
        qty, prod[4], discount, amount, status
    ))

write_csv(
    f"{OUT_DIR}/sales_transactions.csv",
    ["transaction_id","customer_id","product_id","store_id",
     "transaction_date","quantity","unit_price","discount","total_amount","status"],
    transactions,
)

print("\nData generation complete.")
print(f"  Stores:       {len(STORES):>6,}")
print(f"  Products:     {len(PRODUCTS):>6,}")
print(f"  Customers:    {len(CUSTOMERS):>6,}")
print(f"  Transactions: {len(transactions):>6,}")
