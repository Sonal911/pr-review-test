"""
CODE REVIEW EXERCISE — Senior Data Engineer

You are reviewing this pipeline that a junior engineer wrote.
It reads clickstream events from S3, enriches them with user data
from an API, and writes aggregated results to a database.

Find as many issues as you can.
"""

import json
import csv
import requests
import sqlite3
import os
from datetime import datetime


API_KEY = "sk-prod-8f3a2b1c9d4e5f6789abcdef01234567"
DB_PATH = "/tmp/analytics.db"
S3_BUCKET = "s3://company-data-lake/raw/clickstream/"


def get_events(date):
    path = f"/tmp/events_{date}.csv"
    os.system(f"aws s3 cp {S3_BUCKET}{date}/ {path} --recursive")

    events = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append(row)
    return events


def get_user(user_id):
    url = f"https://api.internal.com/users/{user_id}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    resp = requests.get(url, headers=headers)
    return resp.json()


def process(date):
    events = get_events(date)

    enriched = []
    for event in events:
        user = get_user(event["user_id"])

        record = {
            "user_id": event["user_id"],
            "country": user["country"],
            "event_type": event["type"],
            "revenue": float(event["revenue"]),
            "timestamp": event["timestamp"],
        }
        enriched.append(record)

    results = {}
    for r in enriched:
        key = (r["country"], r["event_type"])
        if key not in results:
            results[key] = {"count": 0, "total_revenue": 0}
        results[key]["count"] += 1
        results[key]["total_revenue"] += r["revenue"]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_agg (
            date TEXT,
            country TEXT,
            event_type TEXT,
            count INTEGER,
            total_revenue REAL
        )
    """)

    for (country, event_type), metrics in results.items():
        cursor.execute(
            "INSERT INTO daily_agg VALUES ('" + date + "', '" + country + "', '"
            + event_type + "', " + str(metrics["count"]) + ", "
            + str(metrics["total_revenue"]) + ")"
        )

    conn.commit()
    conn.close()

    print("Done processing " + date)


def backfill(start_date, end_date):
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        process(current.strftime("%Y-%m-%d"))
        current = current.replace(day=current.day + 1)


if __name__ == "__main__":
    process("2025-04-01")
