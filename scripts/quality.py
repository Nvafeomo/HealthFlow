"""
Data quality checks for cdc_weekly_deaths table.
Raises RuntimeError if any check fails, which causes Airflow to mark the task as failed.
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ["HEALTHFLOW_DB_URL"]


def check_row_count(conn, min_rows: int = 1000):
    """Fail if the table has fewer rows than expected."""
    count = conn.execute(text("SELECT COUNT(*) FROM cdc_weekly_deaths")).scalar()
    print(f"[quality] Row count: {count}")
    if count < min_rows:
        raise RuntimeError(f"Row count {count} is below minimum threshold {min_rows}")


def check_null_rate(conn, column: str, max_null_pct: float = 0.05):
    """Fail if more than max_null_pct of a critical column is NULL."""
    result = conn.execute(text(f"""
        SELECT
            COUNT(*) FILTER (WHERE {column} IS NULL)::float / COUNT(*) AS null_rate
        FROM cdc_weekly_deaths
    """)).scalar()
    print(f"[quality] Null rate for '{column}': {result:.2%}")
    if result > max_null_pct:
        raise RuntimeError(
            f"Column '{column}' null rate {result:.2%} exceeds max allowed {max_null_pct:.2%}"
        )


def check_freshness(conn, max_stale_days: int = 30):
    """Fail if the most recent week_ending_date is older than max_stale_days."""
    latest = conn.execute(
        text("SELECT MAX(week_ending_date) FROM cdc_weekly_deaths")
    ).scalar()
    print(f"[quality] Most recent week_ending_date: {latest}")
    if latest is None:
        raise RuntimeError("No rows found — freshness check cannot run")
    cutoff = datetime.utcnow().date() - timedelta(days=max_stale_days)
    if latest < cutoff:
        raise RuntimeError(
            f"Most recent data ({latest}) is older than {max_stale_days} days (cutoff: {cutoff})"
        )


def run_quality_checks():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        check_row_count(conn)
        check_null_rate(conn, "all_cause_deaths")
        check_null_rate(conn, "jurisdiction")
        check_freshness(conn)
    print("[quality] All checks passed.")


if __name__ == "__main__":
    run_quality_checks()
