# MediTrack360 Data Dictionary

## Overview
This document describes the data models used in the MediTrack360 healthcare data platform. The platform follows a star schema with dimension and fact tables optimized for analytical queries.

---

## Dimension Tables

### dim_patient
Patient demographic information and attributes.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| patient_key | BIGINT | Surrogate key for patient dimension | PRIMARY KEY |
| patient_id | INTEGER | Source system patient identifier | NOT NULL |
| first_name | VARCHAR(100) | Patient's first name | |
| last_name | VARCHAR(100) | Patient's last name | |
| birth_date | DATE | Patient's date of birth | |
| age | INTEGER | Calculated age in years | CHECK (age >= 0 AND age <= 120) |
| gender_standardized | VARCHAR(1) | Standardized gender code (M/F/O) | CHECK (gender IN ('M', 'F', 'O')) |
| blood_type | VARCHAR(5) | Patient's blood type | |
| phone_clean | VARCHAR(20) | Cleaned phone number | |
| email | VARCHAR(255) | Patient email address | |
| emergency_contact | VARCHAR(100) | Emergency contact name | |
| age_valid | BOOLEAN | Flag indicating if age is valid | |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |
| current_row_flag | INTEGER | Flag for current row (1=current) | DEFAULT 1 |
| row_expired_at | TIMESTAMP | When this row was expired | |

**Relationships:**
- `fact_admissions.patient_id` → `dim_patient.patient_id`
- `fact_vital_signs.patient_id` → `dim_patient.patient_id`

### dim_ward
Hospital ward information and capacity.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| ward_key | BIGINT | Surrogate key for ward dimension | PRIMARY KEY |
| ward_id | INTEGER | Source system ward identifier | NOT NULL |
| ward_name | VARCHAR(100) | Name of the ward | NOT NULL |
| ward_type | VARCHAR(50) | Type of ward | CHECK (ward_type IN ('General', 'ICU', 'Emergency', 'Pediatric', 'Maternity', 'Surgical')) |
| ward_category | VARCHAR(50) | Category (Critical/Specialized/General) | |
| total_beds | INTEGER | Total number of beds in ward | CHECK (total_beds >= 0) |
| available_beds | INTEGER | Currently available beds | CHECK (available_beds >= 0 AND available_beds <= total_beds) |
| beds_occupied | INTEGER | Currently occupied beds | CHECK (beds_occupied >= 0 AND beds_occupied <= total_beds) |
| utilization_rate | DECIMAL(5,2) | Bed utilization percentage | CHECK (utilization_rate >= 0 AND utilization_rate <= 100) |
| head_nurse | VARCHAR(100) | Name of head nurse | |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Relationships:**
- `fact_admissions.ward_id` → `dim_ward.ward_id`
- `fact_bed_occupancy.ward_id` → `dim_ward.ward_id`

### dim_date
Date dimension for time-based analysis.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| date_key | INTEGER | Date key in YYYYMMDD format | PRIMARY KEY |
| full_date | DATE | Calendar date | NOT NULL |
| year | INTEGER | Year component | |
| quarter | INTEGER | Quarter (1-4) | CHECK (quarter BETWEEN 1 AND 4) |
| month | INTEGER | Month (1-12) | CHECK (month BETWEEN 1 AND 12) |
| day | INTEGER | Day of month | CHECK (day BETWEEN 1 AND 31) |
| day_of_week | INTEGER | Day of week (1=Sunday) | CHECK (day_of_week BETWEEN 1 AND 7) |
| day_of_year | INTEGER | Day of year | CHECK (day_of_year BETWEEN 1 AND 366) |
| week_of_year | INTEGER | Week number | CHECK (week_of_year BETWEEN 1 AND 53) |
| day_type | VARCHAR(10) | Weekday or Weekend | CHECK (day_type IN ('Weekday', 'Weekend')) |
| month_name | VARCHAR(20) | Full month name | |
| day_name | VARCHAR(20) | Full day name | |
| month_end_date | DATE | Last day of the month | |
| days_until_month_end | INTEGER | Days until end of month | |

**Relationships:**
- `fact_admissions.admission_date_key` → `dim_date.date_key`
- `kpi_daily_operations.date_key` → `dim_date.date_key`

### dim_drug
Pharmacy drug information.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| drug_key | BIGINT | Surrogate key for drug dimension | PRIMARY KEY |
| drug_name | VARCHAR(100) | Brand name of the drug | NOT NULL |
| generic_name | VARCHAR(100) | Generic name of the drug | |
| drug_category | VARCHAR(50) | Drug category | CHECK (drug_category IN ('Antibiotic', 'Analgesic', 'Diabetes', 'Other')) |
| unit_price | DECIMAL(10,2) | Price per unit | CHECK (unit_price >= 0) |
| reorder_level | INTEGER | Minimum stock level before reorder | CHECK (reorder_level >= 0) |
| supplier_name | VARCHAR(100) | Drug supplier | |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Relationships:**
- `fact_pharmacy_inventory.drug_name` → `dim_drug.drug_name`

### dim_time
Time dimension for hourly analysis.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| time_key | INTEGER | Hour of day (0-23) | PRIMARY KEY |
| hour | INTEGER | Hour (0-23) | CHECK (hour BETWEEN 0 AND 23) |
| hour_display | VARCHAR(5) | Display format (HH:00) | |
| time_of_day | VARCHAR(20) | Time period | CHECK (time_of_day IN ('Morning', 'Afternoon', 'Evening', 'Night')) |
| business_hours_flag | VARCHAR(20) | Business hours indicator | CHECK (business_hours_flag IN ('Business Hours', 'Non-Business Hours')) |

**Relationships:**
- `fact_admissions.admission_time_key` → `dim_time.time_key`
- `fact_vital_signs.measurement_time_key` → `dim_time.time_key`

---

## Fact Tables

### fact_admissions
Patient admission events and details.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| admission_key | BIGINT | Surrogate key for admission | PRIMARY KEY |
| patient_id | INTEGER | Patient identifier | NOT NULL |
| ward_id | INTEGER | Ward identifier | |
| admission_timestamp | TIMESTAMP | Date and time of admission | NOT NULL |
| admission_date_key | INTEGER | Date key for admission | FOREIGN KEY REFERENCES dim_date(date_key) |
| admission_time_key | INTEGER | Time key for admission | FOREIGN KEY REFERENCES dim_time(time_key) |
| triage_level | INTEGER | Triage severity level (1-5) | CHECK (triage_level BETWEEN 1 AND 5) |
| admission_status | VARCHAR(20) | Current admission status | CHECK (admission_status IN ('Active', 'Discharged', 'Transferred')) |
| attending_doctor | VARCHAR(100) | Name of attending doctor | |
| bed_number_clean | VARCHAR(10) | Cleaned bed number | |
| occupancy_hours | DECIMAL(10,2) | Hours bed was occupied | CHECK (occupancy_hours >= 0) |
| is_discharged | INTEGER | Flag for discharged patients (1=discharged) | CHECK (is_discharged IN (0, 1)) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Grain:** One row per patient admission

### fact_vital_signs
Patient vital sign measurements.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| vital_key | BIGINT | Surrogate key for vital sign | PRIMARY KEY |
| patient_id | INTEGER | Patient identifier | NOT NULL |
| admission_id | INTEGER | Admission identifier | |
| measurement_timestamp | TIMESTAMP | Date and time of measurement | NOT NULL |
| measurement_date_key | INTEGER | Date key for measurement | FOREIGN KEY REFERENCES dim_date(date_key) |
| measurement_time_key | INTEGER | Time key for measurement | FOREIGN KEY REFERENCES dim_time(time_key) |
| heart_rate | INTEGER | Heart rate in BPM | CHECK (heart_rate BETWEEN 30 AND 250) |
| blood_pressure_systolic | INTEGER | Systolic blood pressure | CHECK (blood_pressure_systolic BETWEEN 70 AND 250) |
| blood_pressure_diastolic | INTEGER | Diastolic blood pressure | CHECK (blood_pressure_diastolic BETWEEN 40 AND 150) |
| temperature | DECIMAL(4,1) | Body temperature in Celsius | CHECK (temperature BETWEEN 30 AND 45) |
| oxygen_saturation | INTEGER | Oxygen saturation percentage | CHECK (oxygen_saturation BETWEEN 70 AND 100) |
| respiratory_rate | INTEGER | Respiratory rate per minute | CHECK (respiratory_rate BETWEEN 5 AND 60) |
| ews_score | INTEGER | Early Warning Score | CHECK (ews_score >= 0) |
| is_critical | BOOLEAN | Flag for critical vital signs | |
| bp_category | VARCHAR(50) | Blood pressure category | CHECK (bp_category IN ('Normal', 'Elevated', 'Stage 1 Hypertension', 'Stage 2 Hypertension', 'Unknown')) |
| recorded_hour | INTEGER | Hour of recording | CHECK (recorded_hour BETWEEN 0 AND 23) |
| age_at_recording | INTEGER | Patient age at time of recording | CHECK (age_at_recording >= 0 AND age_at_recording <= 120) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Grain:** One row per vital sign measurement

### fact_lab_results
Laboratory test results.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| lab_result_key | BIGINT | Surrogate key for lab result | PRIMARY KEY |
| patient_id | INTEGER | Patient identifier | NOT NULL |
| sample_timestamp | TIMESTAMP | Date and time of sample collection | NOT NULL |
| sample_date_key | INTEGER | Date key for sample | FOREIGN KEY REFERENCES dim_date(date_key) |
| completion_timestamp | TIMESTAMP | Date and time of result completion | |
| test_name | VARCHAR(200) | Name of the test | NOT NULL |
| test_category | VARCHAR(50) | Test category | CHECK (test_category IN ('Blood Test', 'Urine Test', 'Imaging', 'Other Test')) |
| result | TEXT | Test result | |
| turnaround_minutes | DECIMAL(10,2) | Time from sample to result in minutes | CHECK (turnaround_minutes >= 0) |
| is_delayed | BOOLEAN | Flag for delayed results (> 2 hours) | |
| status_standardized | VARCHAR(20) | Standardized test status | CHECK (status_standardized IN ('Pending', 'In Progress', 'Completed', 'Cancelled')) |
| is_completed | INTEGER | Flag for completed tests (1=completed) | CHECK (is_completed IN (0, 1)) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Grain:** One row per laboratory test

### fact_pharmacy_inventory
Pharmacy drug inventory snapshots.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| inventory_key | BIGINT | Surrogate key for inventory | PRIMARY KEY |
| drug_name | VARCHAR(100) | Name of the drug | NOT NULL |
| snapshot_date | DATE | Date of inventory snapshot | NOT NULL |
| snapshot_date_key | INTEGER | Date key for snapshot | FOREIGN KEY REFERENCES dim_date(date_key) |
| stock_on_hand | INTEGER | Current stock quantity | CHECK (stock_on_hand >= 0) |
| reorder_level | INTEGER | Reorder threshold | CHECK (reorder_level >= 0) |
| unit_price | DECIMAL(10,2) | Price per unit | CHECK (unit_price >= 0) |
| stock_value | DECIMAL(15,2) | Total value of stock (stock_on_hand × unit_price) | CHECK (stock_value >= 0) |
| days_until_expiry | INTEGER | Days until drug expiry | |
| expiring_soon | BOOLEAN | Flag for drugs expiring within 30 days | |
| low_stock | BOOLEAN | Flag for low stock (below reorder level) | |
| low_stock_flag | INTEGER | Integer flag for low stock | CHECK (low_stock_flag IN (0, 1)) |
| expiring_soon_flag | INTEGER | Integer flag for expiring soon | CHECK (expiring_soon_flag IN (0, 1)) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Grain:** One row per drug per snapshot date

### fact_bed_occupancy
Bed occupancy facts.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| occupancy_key | BIGINT | Surrogate key for bed occupancy | PRIMARY KEY |
| ward_id | INTEGER | Ward identifier | NOT NULL |
| occupancy_date_key | INTEGER | Date key for occupancy | FOREIGN KEY REFERENCES dim_date(date_key) |
| bed_number_clean | VARCHAR(10) | Cleaned bed number | NOT NULL |
| status | VARCHAR(20) | Bed status | CHECK (status IN ('Occupied', 'Available', 'Cleaning')) |
| occupancy_hours | DECIMAL(10,2) | Hours bed was occupied | CHECK (occupancy_hours >= 0) |
| is_occupied | INTEGER | Flag for occupied beds (1=occupied) | CHECK (is_occupied IN (0, 1)) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Grain:** One row per bed per day

---

## KPI Tables

### kpi_daily_operations
Daily operational key performance indicators.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| date_key | INTEGER | Date key | FOREIGN KEY REFERENCES dim_date(date_key) |
| ward_id | INTEGER | Ward identifier | FOREIGN KEY REFERENCES dim_ward(ward_id) |
| ward_name | VARCHAR(100) | Ward name | |
| ward_type | VARCHAR(50) | Ward type | |
| total_beds | INTEGER | Total beds in ward | CHECK (total_beds >= 0) |
| admission_count | INTEGER | Number of admissions | CHECK (admission_count >= 0) |
| discharge_count | INTEGER | Number of discharges | CHECK (discharge_count >= 0) |
| occupied_beds | INTEGER | Number of occupied beds | CHECK (occupied_beds >= 0 AND occupied_beds <= total_beds) |
| total_occupancy_hours | DECIMAL(10,2) | Total occupancy hours | CHECK (total_occupancy_hours >= 0) |
| occupancy_rate | DECIMAL(5,2) | Bed occupancy percentage | CHECK (occupancy_rate >= 0 AND occupancy_rate <= 100) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Primary Key:** (date_key, ward_id)

### kpi_patient_care
Daily patient care metrics.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| date_key | INTEGER | Date key | FOREIGN KEY REFERENCES dim_date(date_key) |
| critical_vitals_count | INTEGER | Number of critical vital readings | CHECK (critical_vitals_count >= 0) |
| patients_with_critical_vitals | INTEGER | Number of patients with critical vitals | CHECK (patients_with_critical_vitals >= 0) |
| avg_ews_score | DECIMAL(5,2) | Average Early Warning Score | CHECK (avg_ews_score >= 0) |
| completed_tests | INTEGER | Number of completed lab tests | CHECK (completed_tests >= 0) |
| avg_turnaround_minutes | DECIMAL(10,2) | Average lab turnaround time | CHECK (avg_turnaround_minutes >= 0) |
| delayed_tests_count | INTEGER | Number of delayed lab tests | CHECK (delayed_tests_count >= 0) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Primary Key:** date_key

### kpi_pharmacy_inventory
Daily pharmacy inventory metrics.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| snapshot_date_key | INTEGER | Date key for snapshot | FOREIGN KEY REFERENCES dim_date(date_key) |
| total_drugs | INTEGER | Total number of drugs in inventory | CHECK (total_drugs >= 0) |
| total_inventory_value | DECIMAL(15,2) | Total value of inventory | CHECK (total_inventory_value >= 0) |
| low_stock_items | INTEGER | Number of low stock items | CHECK (low_stock_items >= 0 AND low_stock_items <= total_drugs) |
| expiring_soon_items | INTEGER | Number of items expiring soon | CHECK (expiring_soon_items >= 0 AND expiring_soon_items <= total_drugs) |
| avg_days_until_expiry | DECIMAL(10,2) | Average days until expiry | |
| low_stock_percentage | DECIMAL(5,2) | Percentage of low stock items | CHECK (low_stock_percentage >= 0 AND low_stock_percentage <= 100) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Primary Key:** snapshot_date_key

### patient_summary
Patient-level summary statistics.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| patient_key | BIGINT | Patient key | FOREIGN KEY REFERENCES dim_patient(patient_key) |
| patient_id | INTEGER | Patient identifier | NOT NULL |
| first_name | VARCHAR(100) | Patient first name | |
| last_name | VARCHAR(100) | Patient last name | |
| age | INTEGER | Patient age | CHECK (age >= 0 AND age <= 120) |
| gender_standardized | VARCHAR(1) | Standardized gender | CHECK (gender IN ('M', 'F', 'O')) |
| blood_type | VARCHAR(5) | Blood type | |
| total_admissions | INTEGER | Total number of admissions | CHECK (total_admissions >= 0) |
| total_discharges | INTEGER | Total number of discharges | CHECK (total_discharges >= 0) |
| avg_triage_level | DECIMAL(5,2) | Average triage level | CHECK (avg_triage_level >= 1 AND avg_triage_level <= 5) |
| total_vital_readings | INTEGER | Total vital sign readings | CHECK (total_vital_readings >= 0) |
| critical_readings_count | INTEGER | Number of critical readings | CHECK (critical_readings_count >= 0 AND critical_readings_count <= total_vital_readings) |
| avg_ews_score | DECIMAL(5,2) | Average Early Warning Score | CHECK (avg_ews_score >= 0) |
| avg_heart_rate | DECIMAL(5,2) | Average heart rate | CHECK (avg_heart_rate >= 30 AND avg_heart_rate <= 250) |
| row_created_at | TIMESTAMP | When this row was created | DEFAULT CURRENT_TIMESTAMP |

**Primary Key:** patient_key

---

## Data Quality Rules

### 1. Patient Data
- Age must be between 0 and 120 years
- Gender must be standardized to M/F/O
- Patient ID must be unique and not null

### 2. Vital Signs
- Heart rate: 30-250 BPM
- Oxygen saturation: 70-100%
- Temperature: 30-45°C
- Blood pressure: Systolic > Diastolic

### 3. Admission Data
- Discharge date must be after admission date
- Triage level: 1-5
- Active admissions must have a ward assignment

### 4. Pharmacy Data
- Stock quantity cannot be negative
- Expiry date must be in the future for active drugs
- Unit price must be positive

### 5. Business Rules
- ICU patients must have severity level recorded
- Critical vitals must trigger EWS score calculation
- Lab turnaround time should be less than 4 hours