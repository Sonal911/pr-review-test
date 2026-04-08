
import pandas as pd
import numpy as np
import os
import json
import requests
from datetime import datetime, timedelta

DB_CONNECTION = "postgresql://admin:password123@prod-db.example.com:5432/analytics"
S3_BUCKET = "s3://company-data-lake/raw/"
OUTPUT_PATH = "/tmp/output/"

# ----------------------------
# Data Ingestion
# ----------------------------
def load_csv(path):
    df = pd.read_csv(path)
    return df

def load_from_api(endpoint):
    try:
        resp = requests.get(endpoint)
        data = resp.json()
        df = pd.DataFrame(data)
        return df
    except:
        return pd.DataFrame()

def load_multiple_csvs(directory):
    frames = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, file))
            frames.append(df)
    result = pd.DataFrame()
    for f in frames:
        result = result.append(f)
    return result

# ----------------------------
# Cleaning
# ----------------------------
def clean_users(df):
    df["name"] = df["name"].str.strip()
    df["email"] = df["email"].str.lower()
    df["age"] = df["age"].fillna(0)
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df = df[df["age"] > 0]
    df["age"] = df["age"].astype(int)
    return df

def remove_duplicates(df):
    df = df.drop_duplicates()
    return df

def handle_missing(df):
    for col in df.columns:
        if df[col].dtype == "object":
            df[col].fillna("UNKNOWN", inplace=True)
        else:
            df[col].fillna(0, inplace=True)
    return df

# ----------------------------
# Feature Engineering
# ----------------------------
def add_features(df):
    df["signup_year"] = df["signup_date"].dt.year
    df["signup_month"] = df["signup_date"].dt.month
    df["days_since_signup"] = (datetime.now() - df["signup_date"]).dt.days
    df["is_new_user"] = df["days_since_signup"] < 30

    df["age_group"] = ""
    for i in range(len(df)):
        age = df.iloc[i]["age"]
        if age < 18:
            df.iloc[i, df.columns.get_loc("age_group")] = "minor"
        elif age < 30:
            df.iloc[i, df.columns.get_loc("age_group")] = "young"
        elif age < 50:
            df.iloc[i, df.columns.get_loc("age_group")] = "middle"
        else:
            df.iloc[i, df.columns.get_loc("age_group")] = "senior"

    return df

# ----------------------------
# Aggregation
# ----------------------------
def compute_user_stats(users_df, events_df):
    merged = pd.merge(users_df, events_df, on="user_id")

    stats = merged.groupby("user_id").apply(
        lambda x: pd.Series({
            "total_events": len(x),
            "unique_event_types": x["event_type"].nunique(),
            "first_event": x["event_date"].min(),
            "last_event": x["event_date"].max(),
            "avg_session_duration": x["duration"].mean()
        })
    )

    return stats

def revenue_by_country(orders_df, users_df):
    merged = orders_df.merge(users_df[["user_id", "country"]], on="user_id")
    result = merged.groupby("country")["amount"].sum()
    result = result.sort_values(ascending=False)
    return result

def top_products(orders_df, n=10):
    counts = orders_df.groupby("product_id")["order_id"].count()
    top = counts.nlargest(n)
    return top

# ----------------------------
# Filtering
# ----------------------------
def filter_active_users(df, events_df):
    active_ids = events_df[events_df["event_date"] > "2024-01-01"]["user_id"].unique()
    return df[df["user_id"].isin(active_ids)]

def filter_high_value(orders_df, threshold=100):
    return orders_df[orders_df["amount"] > threshold & orders_df["status"] == "completed"]

# ----------------------------
# Time Series
# ----------------------------
def daily_revenue(orders_df):
    orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])
    daily = orders_df.groupby("order_date")["amount"].sum()
    daily = daily.resample("D").sum()
    return daily

def rolling_average(series, window=7):
    return series.rolling(window).mean()

def compare_periods(df, date_col, value_col):
    df[date_col] = pd.to_datetime(df[date_col])
    current = df[df[date_col] >= datetime.now() - timedelta(days=30)]
    previous = df[(df[date_col] >= datetime.now() - timedelta(days=60)) & 
                  (df[date_col] < datetime.now() - timedelta(days=30))]

    current_total = current[value_col].sum()
    previous_total = previous[value_col].sum()

    change = (current_total - previous_total) / previous_total * 100
    return {"current": current_total, "previous": previous_total, "pct_change": change}

# ----------------------------
# Export
# ----------------------------
def export_to_csv(df, filename):
    df.to_csv(OUTPUT_PATH + filename)

def export_to_json(df, filename):
    data = json.loads(df.to_json())
    with open(OUTPUT_PATH + filename, "w") as f:
        json.dump(data, f)

def generate_report(users_df, orders_df, events_df):
    stats = compute_user_stats(users_df, events_df)
    revenue = revenue_by_country(orders_df, users_df)
    top = top_products(orders_df)

    export_to_csv(stats, "user_stats.csv")
    export_to_csv(revenue.reset_index(), "revenue.csv")
    export_to_csv(top.reset_index(), "top_products.csv")

    return {"stats": stats, "revenue": revenue, "top_products": top}

# ----------------------------
# Data Quality
# ----------------------------
def check_schema(df, expected_columns):
    missing = []
    for col in expected_columns:
        if col not in df.columns:
            missing.append(col)
    if len(missing) > 0:
        print(f"Missing columns: {missing}")
        return False
    return True

def check_nulls(df, critical_columns):
    issues = {}
    for col in critical_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            issues[col] = null_count
    if issues:
        print(f"Null values found: {issues}")
    return issues

def check_uniqueness(df, column):
    total = len(df)
    unique = df[column].nunique()
    duplicates = total - unique
    if duplicates > 0:
        print(f"Found {duplicates} duplicate values in {column}")
    return duplicates

# ----------------------------
# Pipeline
# ----------------------------
def run_pipeline():
    print(f"Pipeline started at {datetime.now()}")

    users = load_csv("data/users.csv")
    orders = load_csv("data/orders.csv")
    events = load_csv("data/events.csv")

    check_schema(users, ["user_id", "name", "email", "age", "signup_date", "country"])
    check_schema(orders, ["order_id", "user_id", "product_id", "amount", "status", "order_date"])
    check_schema(events, ["event_id", "user_id", "event_type", "event_date", "duration"])

    users = clean_users(users)
    users = remove_duplicates(users)
    users = handle_missing(users)
    users = add_features(users)

    orders = handle_missing(orders)
    events = handle_missing(events)

    report = generate_report(users, orders, events)

    active_users = filter_active_users(users, events)
    high_value = filter_high_value(orders)
    daily_rev = daily_revenue(orders)
    rolling_rev = rolling_average(daily_rev)

    export_to_csv(active_users, "active_users.csv")
    export_to_csv(high_value, "high_value_orders.csv")
    export_to_csv(rolling_rev.reset_index(), "rolling_revenue.csv")

    print(f"Pipeline finished at {datetime.now()}")
    return report


if __name__ == "__main__":
    result = run_pipeline()
    print("Done")
    print(f"Stats shape: {result['stats'].shape}")
    print(f"Top country: {result['revenue'].index[0]}")
