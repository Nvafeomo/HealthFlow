# HealthFlow

A end-to-end ETL pipeline that ingests weekly CDC mortality data, transforms it in Databricks using PySpark, and loads it into PostgreSQL — orchestrated by Apache Airflow.

## Architecture

```
CDC Socrata API
      │
      ▼
 extract.py          ← pulls raw weekly death counts, saves CSV locally
      │
      ▼
Databricks (PySpark) ← renames columns, casts types, drops nulls, writes clean CSV
      │  (via Unity Catalog Volume)
      ▼
  load.py            ← upserts clean data into PostgreSQL
      │
      ▼
 quality.py          ← row count, null rate, freshness checks
```

All four tasks run as an Airflow DAG (`healthflow_pipeline`) on a weekly schedule.

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.8 (Docker) |
| Extract | Python + Socrata REST API |
| Transform | Databricks (PySpark, serverless) |
| Storage | Databricks Unity Catalog Volume |
| Load | PostgreSQL + SQLAlchemy |
| Quality | Custom Python checks |
| Testing | pytest |

## Project Structure

```
HealthFlow/
├── dags/
│   └── healthflow_dag.py      # Airflow DAG wiring all 4 tasks
├── scripts/
│   ├── extract.py             # CDC API → local CSV
│   ├── dbfs_utils.py          # Upload/download to Unity Catalog Volume
│   ├── load.py                # CSV → PostgreSQL upsert
│   ├── quality.py             # Post-load data quality checks
│   └── schema.sql             # CREATE TABLE statement
├── notebooks/
│   └── transform.py           # Local copy of Databricks notebook
├── tests/
│   ├── test_extract.py
│   └── test_load.py
├── docker/
│   └── docker-compose.yml     # Airflow + Postgres services
├── data/staging/              # Local staging area (git-ignored)
├── .env.example               # Environment variable template
├── requirements.txt
└── conftest.py
```

## Setup

### Prerequisites

- Docker Desktop
- Python 3.10+
- A [Databricks](https://databricks.com) workspace (serverless)
- A [data.cdc.gov](https://data.cdc.gov) account and app token

### 1. Clone and configure environment

```bash
git clone <your-repo-url>
cd HealthFlow
cp .env.example .env
```

Fill in your `.env`:

```
DATABRICKS_TOKEN=your_token
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_JOB_ID=your_job_id
HEALTHFLOW_DB_URL=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
SOCRATA_APP_TOKEN=your_token
```

### 2. Start Airflow

```bash
cd docker
docker compose up airflow-init
docker compose up -d
```

Airflow UI → http://localhost:8080 (admin / admin)

### 3. Set up Databricks

1. Upload `notebooks/transform.py` content to a Databricks notebook named `healthflow_transform`
2. Create a Unity Catalog Volume at `workspace.default.healthflow_staging`
3. Create a Databricks Job pointing to the notebook (serverless cluster)
4. Copy the Job ID into `DATABRICKS_JOB_ID` in your `.env`

### 4. Initialize the database

```bash
# Stop local Postgres if running on port 5432
Stop-Service postgresql-x64-18   # Windows PowerShell

# Apply schema
Get-Content scripts/schema.sql | docker exec -i docker-postgres-1 psql -U airflow -d airflow
```

### 5. Run tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Running the Pipeline

**Manually trigger a run:**

1. Open Airflow UI → DAGs → `healthflow_pipeline`
2. Click the play button → Trigger DAG

**Scheduled:** The DAG runs automatically every week (`@weekly`).

## Data Source

[CDC Weekly Provisional Counts of Deaths](https://data.cdc.gov/resource/muzy-jte6.json) — dataset `muzy-jte6`

Covers weekly death counts by jurisdiction for all causes, natural causes, heart disease, and COVID-19.

## Key Design Decisions

- **Upsert over insert**: `ON CONFLICT (year, week, jurisdiction) DO UPDATE` ensures re-runs don't create duplicates
- **Unity Catalog Volumes**: Used instead of DBFS (which is disabled in newer Databricks workspaces) as the handoff point between local Airflow and Databricks
- **Serverless cluster**: No cluster management required; Databricks handles compute automatically
- **Quality gate**: Pipeline fails loudly if data doesn't meet row count, null rate, or freshness thresholds — silent bad data is worse than a failed run
