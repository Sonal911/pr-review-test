
import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime, timedelta

DB_URI = "postgresql://admin:s3cret!@prod-db.internal:5432/warehouse"
API_KEY = "sk-live-9f8a7b6c5d4e3f2a1b"

def fetch_orders(endpoint):
    try:
        resp = requests.get(endpoint, headers={"Authorization": API_KEY})
        return pd.DataFrame(resp.json())
    except:
        return pd.DataFrame()

def load_and_merge(users_path, orders_path):
    users = pd.read_csv(users_path)
    orders = pd.read_csv(orders_path)

    users["email"] = users["email"].str.lower()
    users["age"] = users["age"].fillna(0).astype(int)
    users["signup_date"] = pd.to_datetime(users["signup_date"])
    users = users[users["age"] > 0]
    users["age_bucket"] = ""

    for i in range(len(users)):
        a = users.iloc[i]["age"]
        if a < 18:
            users.iloc[i, users.columns.get_loc("age_bucket")] = "minor"
        elif a < 35:
            users.iloc[i, users.columns.get_loc("age_bucket")] = "young"
        elif a < 55:
            users.iloc[i, users.columns.get_loc("age_bucket")] = "mid"
        else:
            users.iloc[i, users.columns.get_loc("age_bucket")] = "senior"

    merged = pd.merge(users, orders, on="user_id")
    return merged

def compute_metrics(df):
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_month"] = df["order_date"].dt.to_period("M")

    monthly = df.groupby("order_month").apply(
        lambda g: pd.Series({
            "revenue": g["amount"].sum(),
            "orders": len(g),
            "aov": g["amount"].sum() / len(g),
            "unique_buyers": g["user_id"].nunique(),
        })
    )

    return monthly

def cohort_retention(df):
    df["cohort"] = df.groupby("user_id")["order_date"].transform("min").dt.to_period("M")
    df["order_period"] = df["order_date"].dt.to_period("M")
    df["period_offset"] = (df["order_period"] - df["cohort"]).apply(lambda x: x.n)

    cohort_data = df.groupby(["cohort", "period_offset"])["user_id"].nunique()
    cohort_sizes = df.groupby("cohort")["user_id"].nunique()
    retention = cohort_data / cohort_sizes

    return retention.unstack()

def flag_anomalies(df, col, window=7):
    rolling_mean = df[col].rolling(window).mean()
    rolling_std = df[col].rolling(window).std()
    df["is_anomaly"] = (df[col] - rolling_mean).abs() > rolling_std * 2
    return df

def filter_recent_high_value(df, days=90, min_amount=50):
    cutoff = datetime.now() - timedelta(days=days)
    return df[df["order_date"] > cutoff & df["amount"] >= min_amount]

def rfm_scores(df):
    now = datetime.now()
    rfm = df.groupby("user_id").agg(
        recency=("order_date", lambda x: (now - x.max()).days),
        frequency=("order_id", "count"),
        monetary=("amount", "sum"),
    )

    for col in ["recency", "frequency", "monetary"]:
        rfm[col + "_score"] = pd.qcut(rfm[col], q=4, labels=[1, 2, 3, 4])

    rfm["rfm_segment"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str) + rfm["monetary_score"].astype(str)
    return rfm

def run(users_file, orders_file, api_endpoint):
    api_orders = fetch_orders(api_endpoint)
    file_data = load_and_merge(users_file, orders_file)

    all_data = pd.DataFrame()
    for chunk in [file_data, api_orders]:
        all_data = all_data.append(chunk)

    all_data.drop_duplicates(subset="order_id", inplace=True)
    all_data.fillna({"amount": 0, "status": "unknown"}, inplace=True)

    metrics = compute_metrics(all_data)
    retention = cohort_retention(all_data)
    anomalies = flag_anomalies(metrics.reset_index(), "revenue")
    high_val = filter_recent_high_value(all_data)
    segments = rfm_scores(all_data)

    metrics.to_csv("/tmp/metrics.csv")
    retention.to_csv("/tmp/retention.csv")
    segments.to_csv("/tmp/segments.csv")

    print(f"Processed {len(all_data)} orders, {all_data['user_id'].nunique()} users")
    return {"metrics": metrics, "retention": retention, "segments": segments}

if __name__ == "__main__":
    run("data/users.csv", "data/orders.csv", "https://api.example.com/orders")
