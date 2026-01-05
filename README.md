# MediTrack360 Data Platform

## Architecture
- Sources: Postgres, API, CSV, Kafka
- Orchestrator: Airflow
- Data Lake: S3 (Bronze/Silver/Gold)
- Transform: Spark
- Quality: GE-style checks
- Warehouse: Redshift
- BI: Power BI / Tableau

## Run (local dev)
1. Set environment variables:
   - S3_BUCKET, AWS_REGION, AWS creds
2. Start stack:
   docker compose -f docker/docker-compose.yml up --build
3. Open Airflow:
   http://localhost:8080
4. Trigger DAG:
   `meditrack360_end_to_end`

## Project
Cloud-native healthcare data platform with end-to-end ETL pipelines following Bronze–Silver–Gold lakehouse architecture.
