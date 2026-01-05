import os
import io
from datetime import datetime

import pandas as pd
import boto3
from sqlalchemy import create_engine

# -----------------------------
# 1) Postgres connection (Supabase)
# -----------------------------
PG_USER = "medi_reader.hceprxhtdgtbqmrfwymn"
PG_PWD  = "medi_reader123"  # rotate + move to .env later
PG_HOST = "aws-1-eu-central-1.pooler.supabase.com"
PG_PORT = 5432
PG_DB   = "postgres"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    connect_args={"sslmode": "require"},
    pool_pre_ping=True,
)

# -----------------------------
# 2) S3 target (Bronze)
# -----------------------------
AWS_REGION = "eu-central-1"
S3_BUCKET = "meditrack360-data-lake-6065273c"
BRONZE_PREFIX = "bronze/postgres"

s3 = boto3.client("s3", region_name=AWS_REGION)

run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

# -----------------------------
# 3) Extract tables
# -----------------------------
tables = {
    "patients":   'SELECT * FROM public.patients',
    "admissions": 'SELECT * FROM public.admissions',
    "discharges": 'SELECT * FROM public.discharges',
    "wards":      'SELECT * FROM public.wards',
}

# -----------------------------
# 4) Upload each table to S3 (CSV in-memory)
# -----------------------------
for name, query in tables.items():
    df = pd.read_sql(query, engine)

    buf = io.StringIO()
    df.to_csv(buf, index=False)

    key = f"{BRONZE_PREFIX}/{name}/dt={run_id}/{name}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"))

    print(f"✅ Uploaded {name}: rows={len(df)} -> s3://{S3_BUCKET}/{key}")

print("\n🎉 Done. Bronze layer written successfully.")
