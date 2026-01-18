# Daily Admissions by ward
CREATE OR REPLACE VIEW gold.kpi_daily_admissions_by_ward AS
SELECT
  date_format(a.admission_time, '%Y%m%d') AS date_key_str,
  CAST(date_format(a.admission_time, '%Y%m%d') AS integer) AS date_key,
  a.ward_id,
  w.ward_name,
  COUNT(*) AS admissions
FROM gold.fact_admissions a
LEFT JOIN gold.dim_ward w
  ON a.ward_id = w.ward_id
GROUP BY
  CAST(date_format(a.admission_time, '%Y%m%d') AS integer),
  date_format(a.admission_time, '%Y%m%d'),
  a.ward_id,
  w.ward_name;


# Avg Lab Turnaround per Day
CREATE OR REPLACE VIEW gold.kpi_daily_lab_turnaround AS
SELECT
  CAST(date_format(sample_time, '%Y%m%d') AS integer) AS date_key,
  COUNT(*) AS tests_count,
  AVG(turnaround_minutes) AS avg_turnaround_minutes,
  SUM(CASE WHEN turnaround_minutes > 120 THEN 1 ELSE 0 END) AS delayed_tests_over_120m
FROM gold.fact_lab_turnaround
GROUP BY CAST(date_format(sample_time, '%Y%m%d') AS integer);

# Pharmacy Low Stock Count
CREATE OR REPLACE VIEW gold.kpi_pharmacy_low_stock AS
SELECT
  COUNT(*) AS total_items,
  SUM(CASE WHEN stock_on_hand <= reorder_level THEN 1 ELSE 0 END) AS low_stock_items,
  (SUM(CASE WHEN stock_on_hand <= reorder_level THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS low_stock_pct
FROM gold.fact_pharmacy_stock;


# ICU Alerts per Day 
CREATE OR REPLACE VIEW gold.kpi_daily_icu_alerts AS
SELECT
  CAST(date_format(alert_time, '%Y%m%d') AS integer) AS date_key,
  alert_type,
  severity,
  COUNT(*) AS alerts_count
FROM gold.fact_icu_alerts
GROUP BY
  CAST(date_format(alert_time, '%Y%m%d') AS integer),
  alert_type,
  severity;

