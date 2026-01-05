
## Technology Stack

### Infrastructure
- **Terraform**: Infrastructure as Code (AWS resources)
- **Docker**: Containerization for local development
- **Docker Compose**: Multi-container orchestration

### Data Processing
- **Apache Airflow**: Workflow orchestration and scheduling
- **Apache Spark**: Distributed data processing (PySpark)
- **PostgreSQL**: Source database (hospital data)
- **Amazon S3**: Data lake storage (Bronze/Silver/Gold layers)

### Data Warehouse & Analytics
- **Amazon Redshift**: Cloud data warehouse
- **Great Expectations**: Data quality validation
- **SQL**: Analytics and reporting

### Development & Operations
- **Python 3.9**: Primary programming language
- **pytest**: Testing framework
- **GitHub Actions**: CI/CD pipeline
- **Jupyter Notebooks**: Data exploration and analysis

## Data Flow

### 1. Extraction (Batch)
- **PostgreSQL**: Patient admissions, wards, bed assignments, vital signs
- **Pharmacy API**: Drug inventory and stock levels
- **Labs CSV**: Laboratory test results
- **Frequency**: Hourly batches

### 2. Bronze Layer (Raw)
- Raw data stored in S3 as Parquet files
- No transformations applied
- Retention: 365 days with lifecycle policies

### 3. Silver Layer (Cleaned)
- Data cleaning and standardization
- Healthcare-specific validations
- Joined and enriched data
- Retention: 730 days

### 4. Gold Layer (Analytics-ready)
- Dimension tables (patients, wards, dates, drugs, time)
- Fact tables (admissions, vitals, labs, pharmacy, beds)
- Aggregated KPI tables
- Ready for reporting and analytics

### 5. Data Quality
- **Bronze Validation**: Schema validation, null checks, data type validation
- **Silver Validation**: Business rules, healthcare-specific validations
- **Great Expectations**: Automated data quality testing

## Key Design Decisions

### 1. Medallion Architecture
- **Why**: Clear separation of concerns, incremental data quality improvements
- **Benefit**: Easy debugging, data lineage, and reprocessing capabilities

### 2. Batch Processing (not Streaming)
- **Why**: Hospital data doesn't require real-time processing
- **Benefit**: Simplified architecture, easier error handling, cost-effective

### 3. Healthcare-Specific Validations
- **Why**: Medical data requires strict quality controls
- **Benefit**: Patient safety, regulatory compliance, data reliability

### 4. AWS Cloud Native
- **Why**: Scalability, managed services, enterprise readiness
- **Benefit**: Reduced operational overhead, integration with other AWS services

## Scalability Considerations

### Horizontal Scaling
- Spark workers can be added based on data volume
- Redshift can scale compute nodes independently
- S3 provides unlimited storage scalability

### Performance Optimizations
- Partitioning by date in S3
- Sort keys in Redshift for query performance
- Spark optimizations for healthcare data patterns

### Cost Optimization
- S3 lifecycle policies for automatic archival
- Redshift pause/resume for non-production environments
- Spot instances for Spark workers where appropriate

## Security & Compliance

### Data Security
- Encryption at rest (S3, Redshift)
- Encryption in transit (TLS for all connections)
- IAM roles for least privilege access

### Healthcare Compliance
- Data anonymization in analytical layers
- Audit logging for all data accesses
- Access controls for sensitive health data

### Infrastructure Security
- VPC isolation for Redshift
- Security groups for network access control
- Regular security patching via CI/CD