# Cloud E-Commerce Data Lake Pipeline

An end-to-end data engineering pipeline that processes large-scale e-commerce transaction data using Python, PySpark, Spark SQL, and a Bronze-Silver-Gold data lake architecture.

The pipeline ingests raw retail data, performs data cleaning and quality processing, creates partitioned Parquet datasets, and generates business-ready analytical datasets.

---

## Architecture

```text
                    Raw E-Commerce Dataset
                            |
                            v
                  +---------------------+
                  | Python Ingestion    |
                  | Pandas + PyArrow    |
                  +----------+----------+
                             |
                             v
                    +----------------+
                    | Bronze Layer   |
                    | Raw Parquet    |
                    +-------+--------+
                            |
                            v
                  +---------------------+
                  | PySpark             |
                  | Transformation      |
                  | Data Cleaning        |
                  | Deduplication       |
                  +----------+----------+
                             |
                             v
                    +----------------+
                    | Silver Layer   |
                    | Partitioned     |
                    | Parquet Data    |
                    +-------+--------+
                            |
                            v
                  +---------------------+
                  | Spark SQL            |
                  | Business Analytics   |
                  +----------+----------+
                             |
                             v
                     +---------------+
                     | Gold Layer    |
                     | Analytics     |
                     +---------------+
                            |
                            v
                  +---------------------+
                  | Airflow DAG          |
                  | Pipeline Orchestration|
                  +---------------------+

Project Overview
This project implements a production-oriented e-commerce data processing workflow using a layered data lake architecture.
The pipeline processes the UCI Online Retail II dataset, containing more than 1 million transaction records.
The project demonstrates practical data engineering concepts including:
- ETL pipeline development
- PySpark transformations
- Spark SQL analytics
- Bronze / Silver / Gold architecture
- Parquet data storage
- Partitioned datasets
- Data cleaning
- Deduplication
- Data validation
- Business analytics
- Pipeline orchestration with Airflow DAG
- Git and GitHub version control
Dataset
The project uses the Online Retail II dataset from the UCI Machine Learning Repository.
Dataset characteristics:
- More than 1 million transaction records
- Customer information
- Product information
- Invoice information
- Quantity and price
- Transaction timestamps
- Country information
Source:
https://archive.ics.uci.edu/dataset/502/online+retail+ii
Tech Stack
Technology	Purpose
Python	Pipeline development
Pandas	Data ingestion and local processing
PySpark	Distributed data transformation
Spark SQL	Analytical queries
PyArrow	Parquet processing
Parquet	Columnar data storage
Apache Airflow	Pipeline orchestration
AWS S3	Planned cloud data lake storage
Amazon Athena	Planned SQL analytics layer
Git	Version control
GitHub	Source code hosting


Data Lake Architecture
The pipeline follows a Medallion Architecture.
Bronze Layer
Contains the ingested raw dataset converted into Parquet format.
Excel Dataset
     |
     v
Python / Pandas
     |
     v
Bronze Parquet

The raw Excel workbook is separated into yearly datasets:
data/bronze/
├── year_2009_2010.parquet
└── year_2010_2011.parquet

Total records ingested:
1,067,371
Silver Layer
The Silver layer contains cleaned and transformed transaction data.
Processing includes:
- Schema normalization
- Data type conversion
- Invalid transaction filtering
- Cancellation / return handling
- Duplicate removal
- Revenue calculation
- Data quality processing
- Year/month partitioning
Example structure:
data/silver/sales/
├── year=2009/
│   └── month=12/
├── year=2010/
│   ├── month=1/
│   ├── month=2/
│   ├── ...
│   └── month=12/
└── year=2011/
    ├── month=1/
    ├── ...
    └── month=12/

Final Silver dataset:
1,007,913 records
Gold Layer
The Gold layer contains business-ready analytical datasets generated using Spark SQL.
Generated datasets:
data/gold/
├── monthly_sales.parquet
├── country_sales.parquet
├── product_sales.parquet
└── customer_sales.parquet

Dataset sizes:
Dataset	Records
Monthly Sales	25
Country Sales	43
Product Sales	4,917
Customer Sales	5,878


ETL Pipeline
1. Ingestion
The ingestion process reads the original Excel workbook using Pandas and converts the yearly sheets into Parquet files.
File:
src/ingestion/ingest_raw.py

Responsibilities:
- Read Excel workbook
- Process multiple sheets
- Normalize schema
- Convert data types
- Write Bronze Parquet datasets
2. Transformation
PySpark is used for the main data transformation process.
File:
src/transformation/silver_transform.py

Transformation operations include:
- Loading Bronze datasets
- Schema handling
- Data cleansing
- Invalid transaction filtering
- Cancellation detection
- Duplicate removal
- Revenue calculation
- Year/month partitioning
- Silver Parquet generation
3. Analytics
Spark SQL is used to generate business-level analytical datasets.
File:
src/analytics/gold_analytics.py

The pipeline generates:
Monthly Sales
Analyzes revenue by month.
Country Sales
Analyzes revenue across countries.
Product Sales
Identifies product-level revenue performance.
Customer Sales
Calculates customer-level revenue.
Data Quality
The pipeline includes validation checks to ensure the generated datasets are usable.
Validation includes:
- Record count validation
- Duplicate detection
- Required field validation
- Quantity validation
- Price validation
- Revenue calculation validation
- Gold dataset existence validation
Verification script:
tests/verify_pipeline.py

Latest successful verification:
✓ monthly_sales: 25 rows
✓ country_sales: 43 rows
✓ product_sales: 4,917 rows
✓ customer_sales: 5,878 rows

Airflow Orchestration
An Apache Airflow DAG definition is included for pipeline orchestration.
DAG:
dags/ecommerce_data_lake_dag.py

Pipeline flow:
Ingestion
    |
    v
Silver Transformation
    |
    v
Gold Analytics

The DAG executes the pipeline stages sequentially.
The DAG file has also been syntax-validated using Python compilation.
Project Structure
cloud-ecommerce-data-lake/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── ingestion/
│   │   └── ingest_raw.py
│   │
│   ├── transformation/
│   │   └── silver_transform.py
│   │
│   └── analytics/
│       └── gold_analytics.py
│
├── dags/
│   └── ecommerce_data_lake_dag.py
│
├── tests/
│   └── verify_pipeline.py
│
├── requirements.txt
├── .gitignore
└── README.md

Key Engineering Highlights
1M+ Records
Processed more than 1 million e-commerce transaction records through the pipeline.
PySpark Processing
Used PySpark for large-scale data transformation and Spark SQL analytics.
Medallion Architecture
Implemented:
Bronze → Silver → Gold

to separate raw, cleaned, and analytical datasets.
Partitioned Storage
Silver datasets are partitioned by:
year / month

to improve organization and downstream query efficiency.
Parquet
Used Parquet as the primary columnar storage format for processed datasets.
Data Quality
Implemented validation and cleansing checks before producing analytical datasets.
Reproducible Pipeline
The pipeline is organized into separate ingestion, transformation, analytics, and orchestration components.
Running the Project
1. Clone the Repository
git clone https://github.com/jemmiii/cloud-ecommerce-data-lake.git
cd cloud-ecommerce-data-lake

2. Create Virtual Environment
python -m venv venv

Activate on Windows:
.\venv\Scripts\Activate.ps1

3. Install Dependencies
pip install -r requirements.txt

4. Run Ingestion
python src/ingestion/ingest_raw.py

5. Run Silver Transformation
python src/transformation/silver_transform.py

6. Run Gold Analytics
python src/analytics/gold_analytics.py

7. Verify Pipeline
python tests/verify_pipeline.py

Cloud Roadmap
The pipeline is designed to extend into a cloud-native data lake architecture.
Planned cloud integration:
Local Pipeline
      |
      v
AWS S3 Data Lake
      |
      v
Amazon Athena
      |
      v
SQL Analytics

Future cloud enhancements include:
- AWS S3 Bronze / Silver / Gold storage
- Amazon Athena SQL analytics
- AWS Glue integration
- Cloud-based orchestration
- Automated monitoring
- Data quality frameworks
- Incremental data processing
Engineering Challenges
Windows + Spark File System Compatibility
During local development, Spark's Hadoop filesystem integration created Windows-specific filesystem issues while writing Parquet files.
The pipeline was designed to continue using Spark for transformation and Spark SQL processing while using PyArrow/Pandas for reliable local Parquet persistence.
This allowed the project to maintain PySpark-based processing without relying on untrusted Windows Hadoop binaries.
Future Improvements
- AWS S3 integration
- Amazon Athena analytics
- AWS Glue Data Catalog
- Incremental ETL processing
- Automated data quality framework
- Cloud-based Airflow deployment
- Pipeline monitoring and alerting
- CI/CD using GitHub Actions
- Data lineage and observability
Author
Jemin Patidar
B.Tech Information Technology
Manipal University Jaipur
GitHub:
https://github.com/jemmiii
Portfolio:
https://jeminpatidar-portfolio.vercel.app/
Project Status
Current Status: Local data lake pipeline completed and verified.
Implemented:
- Python ingestion
- PySpark transformation
- Spark SQL analytics
- Bronze / Silver / Gold architecture
- Partitioned Parquet storage
- Data validation
- Airflow DAG definition
- Git/GitHub version control
