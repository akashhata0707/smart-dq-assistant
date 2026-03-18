import pandas as pd
import numpy as np
import uuid
from datetime import date, datetime, timedelta
import os

# --- Constants ---
TOTAL_ROWS = 1_000_000
NULL_PAYMENT_RATE = 0.15
NEGATIVE_AMOUNT_RATE = 0.05
FUTURE_DATE_RATE = 0.03
INVALID_CATEGORY_RATE = 0.07
DUPLICATE_ID_RATE = 0.02

VALID_CATEGORIES = ["Electronics", "Clothing", "Books", "Food"]
INVALID_CATEGORIES = ["Unknown", "INVALID", "???", "TestCategory"]
PAYMENT_METHODS = ["UPI", "Card", "COD"]
DELIVERY_STATUSES = ["Delivered", "Pending", "Cancelled"]

np.random.seed(42)

# --- Base Generation ---
print("Generating base dataset...")

start_date = date(2024, 1, 1)
end_date = date.today()
date_range_days = (end_date - start_date).days
random_days = np.random.randint(0, date_range_days, TOTAL_ROWS)
order_dates = [start_date + timedelta(days=int(d)) for d in random_days]

data = {
    "order_id":         [str(uuid.uuid4()) for _ in range(TOTAL_ROWS)],
    "customer_id":      [str(uuid.uuid4()) for _ in range(TOTAL_ROWS)],
    "order_amount":     np.round(np.random.uniform(10, 5000, TOTAL_ROWS), 2),
    "order_date":       order_dates,
    "product_category": np.random.choice(VALID_CATEGORIES, TOTAL_ROWS),
    "payment_method":   np.random.choice(PAYMENT_METHODS, TOTAL_ROWS),
    "delivery_status":  np.random.choice(DELIVERY_STATUSES, TOTAL_ROWS),
    "customer_age":     np.random.randint(18, 80, TOTAL_ROWS),
}

df = pd.DataFrame(data)

# --- Bad Data Injection ---
print("Injecting bad data...")

# 1. Null payment_method (15%)
null_idx = df.sample(frac=NULL_PAYMENT_RATE, random_state=42).index
df.loc[null_idx, "payment_method"] = None

# 2. Negative order_amount (5%)
neg_idx = df.sample(frac=NEGATIVE_AMOUNT_RATE, random_state=43).index
df.loc[neg_idx, "order_amount"] = -df.loc[neg_idx, "order_amount"].abs()

# 3. Future order_date (3%)
fut_idx = df.sample(frac=FUTURE_DATE_RATE, random_state=44).index
future_dates = [
    datetime.today().date() + timedelta(days=int(d))
    for d in np.random.randint(1, 60, len(fut_idx))
]
df.loc[fut_idx, "order_date"] = future_dates

# 4. Invalid product_category (7%)
inv_idx = df.sample(frac=INVALID_CATEGORY_RATE, random_state=45).index
df.loc[inv_idx, "product_category"] = np.random.choice(INVALID_CATEGORIES, len(inv_idx))

# 5. Duplicate order_id (2%)
dup_idx = df.sample(frac=DUPLICATE_ID_RATE, random_state=46).index
source_ids = df.loc[~df.index.isin(dup_idx), "order_id"].sample(
    len(dup_idx), random_state=46
).values
df.loc[dup_idx, "order_id"] = source_ids

# --- Verification ---
print("\n--- Injection Verification ---")
print(f"Null payment_method : {df['payment_method'].isna().mean():.2%}  (target: 15%)")
print(f"Negative order_amount: {(df['order_amount'] < 0).mean():.2%}  (target:  5%)")
print(f"Future order_date   : {(pd.to_datetime(df['order_date']) > pd.Timestamp.today()).mean():.2%}  (target:  3%)")
print(f"Invalid category    : {(~df['product_category'].isin(VALID_CATEGORIES)).mean():.2%}  (target:  7%)")
print(f"Duplicate order_id  : {df['order_id'].duplicated().mean():.2%}  (target:  2%)")
print(f"Total rows          : {len(df):,}")

# --- Save ---
os.makedirs("data/raw", exist_ok=True)
df.to_csv("data/raw/orders_dataset.csv", index=False)
print("\nDataset saved to data/raw/orders_dataset.csv")