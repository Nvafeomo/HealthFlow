# Databricks notebook — Python
# This file is a local copy of the notebook at:
# healthflow_transform (workspace/default/healthflow_staging)

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

VOLUME_PATH = "/Volumes/workspace/default/healthflow_staging"

# 1. Read raw CSV from Volume
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(f"{VOLUME_PATH}/raw_cdc.csv"))

# 2. Rename columns to clearer analytics names
rename_map = {
    "jurisdiction_of_occurrence": "jurisdiction",
    "mmwryear": "year",
    "mmwrweek": "week",
    "all_cause": "all_cause_deaths",
    "natural_cause": "natural_cause_deaths",
    "diseases_of_heart_i00_i09": "heart_disease_deaths",
    "covid_19_u071_multiple_cause_of_death": "covid_19_multiple_cause_deaths",
    "covid_19_u071_underlying_cause_of_death": "covid_19_underlying_cause_deaths"
}
for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# 3. Drop nulls in critical columns
df = df.dropna(subset=["year", "week", "jurisdiction", "all_cause_deaths"])

# 4. Cast types
df = (df.withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("week", F.col("week").cast(IntegerType()))
        .withColumn("week_ending_date", F.to_date("week_ending_date"))
        .withColumn("all_cause_deaths", F.col("all_cause_deaths").cast(IntegerType()))
        .withColumn("natural_cause_deaths", F.col("natural_cause_deaths").cast(IntegerType()))
        .withColumn("heart_disease_deaths", F.col("heart_disease_deaths").cast(IntegerType()))
        .withColumn("covid_19_multiple_cause_deaths", F.col("covid_19_multiple_cause_deaths").cast(IntegerType()))
        .withColumn("covid_19_underlying_cause_deaths", F.col("covid_19_underlying_cause_deaths").cast(IntegerType())))

# 5. Add ingestion timestamp
df = df.withColumn("ingested_at", F.current_timestamp())

# 6. Select final columns
df = df.select(
    "year",
    "week",
    "week_ending_date",
    "jurisdiction",
    "all_cause_deaths",
    "natural_cause_deaths",
    "heart_disease_deaths",
    "covid_19_multiple_cause_deaths",
    "covid_19_underlying_cause_deaths",
    "ingested_at"
)

# 7. Write as single CSV file to Volume
df.toPandas().to_csv(f"{VOLUME_PATH}/clean_cdc.csv", index=False)
print(f"Wrote clean rows to {VOLUME_PATH}/clean_cdc.csv")
