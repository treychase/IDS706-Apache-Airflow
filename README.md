# Airflow ETL – MovieLens example

## Overview
This repo contains an Airflow pipeline that:
1. Downloads MovieLens 100K,
2. Transforms ratings and movie files (parallel transforms),
3. Merges and loads the final dataset into PostgreSQL,
4. Performs a simple analysis (top-rated movies plot).

## How to run (local dev)
1. Build and start services:
   ```bash
   docker-compose up --build
   ```
2. Visit Airflow UI: `http://localhost:8080` (login: `admin` / `admin`)
3. Enable and trigger DAG: `movielens_etl_pipeline`
4. After DAG runs, the merged table `movie_ratings_merged` will be in Postgres.
   - Data files and plot: `/opt/airflow/data/` inside the container (mounted from repo).

## Files included
- `docker-compose.yml` — Airflow + Postgres compose
- `dags/movielens_pipeline_dag.py` — Airflow DAG
- `scripts/` — download / transform / load / analysis scripts
- `sql/schema.sql` — example schema
- `devcontainer.json` / `Dockerfile` — dev container support

## Notes / grading checklist
- DAG has schedule and TaskGroups (transform tasks run in parallel).
- Data is not passed between tasks — only file paths via XCom.
- Final merged table is loaded into PostgreSQL.
- Analysis saved a figure to disk (screenshot for submission).
