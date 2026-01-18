# dim_patient table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_patient (
  patient_key        bigint,
  patient_id         bigint,
  first_name         string,
  last_name          string,
  birth_date         date,
  age                int,
  gender             string,
  created_at         timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/dim_patient/';


# dim_ward table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_ward (
  ward_key     bigint,
  ward_id      bigint,
  ward_name    string,
  created_at   timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/dim_ward/';


# dim_date table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_date (
  date_key    int,
  full_date   date,
  year        int,
  month       int,
  day         int,
  day_name    string,
  month_name  string,
  created_at  timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/dim_date/';


# dim_drug table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_drug (
  drug_key    bigint,
  drug_name   string,
  created_at  timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/dim_drug/';


# fact_admissions table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_admissions (
  admission_key   bigint,
  admission_id    bigint,
  patient_id      bigint,
  ward_id         bigint,
  admission_time  timestamp,
  created_at      timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/fact_admissions/';


# fact_lab_turnaround table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_lab_turnaround (
  lab_turnaround_key  bigint,
  patient_id          bigint,
  sample_time         timestamp,
  completed_time      timestamp,
  turnaround_minutes  double,
  created_at          timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/fact_lab_turnaround/';


# fact_pharmacy_stock table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_pharmacy_stock (
  inventory_key   bigint,
  drug_name       string,
  stock_on_hand   int,
  reorder_level   int,
  created_at      timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/fact_pharmacy_stock/';


# fact_icu_alerts table
CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_icu_alerts (
  alert_id    bigint,
  patient_id  bigint,
  alert_time  timestamp,
  alert_type  string,
  severity    string,
  created_at  timestamp
)
STORED AS PARQUET
LOCATION 's3://meditrack360-data-lake-6065273c/gold/fact_icu_alerts/';
