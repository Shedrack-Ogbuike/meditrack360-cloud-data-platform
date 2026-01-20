# MediTrack360 Cloud Analytics Platform

A **production-style healthcare data analytics platform** built using modern **cloud data engineering** practices.

This project demonstrates an **end-to-end data platform**, combining **ETL pipelines, orchestration, analytics, CI/CD, and Infrastructure as Code (IaC)** — designed explicitly as a **portfolio-grade system** aligned with industry standards.

---

## Project Overview

**MediTrack360** ingests healthcare operational data, processes it through a **Bronze → Silver → Gold** data lake architecture, and exposes **analytics-ready datasets** for KPI reporting and dashboards.

The platform mirrors how **real healthcare and enterprise analytics systems** are built and deployed in industry.
---

##  Infrastructure as Code (Terraform)

All cloud infrastructure is defined using **Terraform**, following industry best practices.

Terraform provisions:

* **Amazon S3 buckets** for Bronze, Silver, and Gold layers
* **IAM roles and policies** for secure access
* **AWS Athena** for serverless SQL analytics
* Supporting cloud configurations

### Why Terraform?

* Reproducible environments
* Version-controlled infrastructure
* Industry-standard deployment approach
* Matches real-world data engineering workflows

---

##  Data Pipeline (ETL)

### Bronze Layer (Raw Data)

* Extracts data from:

  * PostgreSQL (patients, admissions, wards)
  * CSV and API sources (pharmacy)
  * Parquet files (lab results)
* Stores raw, unmodified data in **Amazon S3 (Bronze)**

### Silver Layer (Cleaned & Standardized)

* Schema enforcement and type casting
* Timestamp normalization (UTC)
* Deduplication
* Lightweight enrichment
* Outputs Parquet datasets to **Amazon S3 (Silver)**

### Gold Layer (Analytics-Ready)

Creates analytics-focused datasets:

#### Dimension Tables

* `dim_patient`
* `dim_ward`
* `dim_date`
* `dim_drug`

#### Fact Tables

* `fact_admissions`
* `fact_lab_turnaround`
* `fact_pharmacy_stock`
* `fact_icu_alerts`

Gold data remains in **S3** and is queried using **Athena**.

---

## 📊 Analytics & KPIs

The platform supports operational healthcare KPIs such as:

* Bed occupancy rate
* Lab test turnaround time
* Pharmacy stockout risk
* ICU early warning alerts
* Daily admissions by ward
* Patient admission summaries

KPIs are implemented as **SQL views in Athena**, following analytics engineering best practices.

---

## ⚙️ Orchestration (Apache Airflow)

* Airflow runs inside **Docker**
* DAG orchestrates:

  * Extract → Bronze
  * Bronze → Silver (Spark)
  * Silver → Gold (Spark)
* Supports manual and scheduled execution
* Designed for idempotent runs

---

## 🔁 CI/CD & Automation

### GitHub Actions

* Code linting (Black, Flake8)
* Python syntax validation
* Import smoke tests
* Workflow safety checks

### Self-Hosted Runner

* Enables GitHub Actions to trigger **local Airflow DAGs**
* Demonstrates hybrid CI/CD patterns used in industry

> CI validates code quality; Airflow handles production data execution — a common enterprise pattern.

---

## 🧪 Data Quality & Reliability

Includes basic data quality safeguards:

* Schema validation
* Null checks on critical fields
* Safe fallbacks for missing data
* Controlled transformations between layers

---

## 🛠️ Technologies Used

* **Python**
* **Apache Airflow**
* **Apache Spark (PySpark)**
* **Amazon S3**
* **AWS Athena**
* **Terraform**
* **Docker**
* **GitHub Actions**
* **SQL**

---

![1](https://github.com/user-attachments/assets/69181193-6e1e-45fa-9a99-ff801c8bb2c2)
