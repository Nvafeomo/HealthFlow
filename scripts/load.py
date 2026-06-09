import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ["HEALTHFLOW_DB_URL"]


def load(parquet_path: str):
    if parquet_path.endswith(".csv"):
        df = pd.read_csv(parquet_path)
    else:
        df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} rows from {parquet_path}")

    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO cdc_weekly_deaths (
                    year, week, week_ending_date, jurisdiction,
                    all_cause_deaths, natural_cause_deaths, heart_disease_deaths,
                    covid_19_multiple_cause_deaths, covid_19_underlying_cause_deaths,
                    ingested_at
                )
                VALUES (
                    :year, :week, :week_ending_date, :jurisdiction,
                    :all_cause_deaths, :natural_cause_deaths, :heart_disease_deaths,
                    :covid_19_multiple_cause_deaths, :covid_19_underlying_cause_deaths,
                    NOW()
                )
                ON CONFLICT (year, week, jurisdiction)
                DO UPDATE SET
                    all_cause_deaths                = EXCLUDED.all_cause_deaths,
                    natural_cause_deaths            = EXCLUDED.natural_cause_deaths,
                    heart_disease_deaths            = EXCLUDED.heart_disease_deaths,
                    covid_19_multiple_cause_deaths  = EXCLUDED.covid_19_multiple_cause_deaths,
                    covid_19_underlying_cause_deaths = EXCLUDED.covid_19_underlying_cause_deaths,
                    updated_at                      = NOW()
            """), row.to_dict())

    row_count = pd.read_sql("SELECT COUNT(*) FROM cdc_weekly_deaths", engine).iloc[0, 0]
    print(f"Load complete. Total rows in table: {row_count}")


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/staging/clean_cdc.parquet"
    load(path)
