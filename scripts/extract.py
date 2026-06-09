import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data.cdc.gov/resource/muzy-jte6.json"
STAGING_DIR = "data/staging"
LIMIT = 1000
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")


def extract():
    records = []
    offset = 0

    print("Starting CDC API extraction...")

    while True:
        response = requests.get(
            BASE_URL,
            params={"$limit": LIMIT, "$offset": offset},
            headers={
                "User-Agent": "HealthFlow-ETL/1.0 (nvafeomo05@gmail.com)",
                "X-App-Token": APP_TOKEN
            },
            timeout=30
        )
        response.raise_for_status()

        batch = response.json()

        # Empty response means we've fetched everything
        if not batch:
            break

        records.extend(batch)
        offset += LIMIT
        print(f"  Fetched {len(records)} records so far...")

    print(f"Extraction complete. Total records: {len(records)}")

    df = pd.DataFrame(records)

    os.makedirs(STAGING_DIR, exist_ok=True)

    today = datetime.today().strftime("%Y%m%d")
    output_path = f"{STAGING_DIR}/raw_cdc_{today}.csv"
    df.to_csv(output_path, index=False)

    print(f"Saved to {output_path}")
    return output_path


if __name__ == "__main__":
    extract()
