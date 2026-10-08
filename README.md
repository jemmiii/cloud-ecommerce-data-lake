# 🚀 Cloud E-Commerce Data Lake Pipeline

> End-to-end data engineering pipeline for processing 1M+ e-commerce transactions using **Python, PySpark, Spark SQL, Parquet, and Apache Airflow**, following a **Bronze → Silver → Gold data lake architecture**.

## 🏗️ Architecture

```text
                 UCI Online Retail II
                         │
                         ▼
                ┌─────────────────┐
                │ Python Ingestion │
                │ Pandas + PyArrow │
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │  🥉 BRONZE      │
                │ Raw Parquet     │
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │    PySpark      │
                │ Cleaning & ETL  │
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │  🥈 SILVER      │
                │ Clean +         │
                │ Partitioned     │
                │ Parquet         │
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │   Spark SQL     │
                │   Analytics     │
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │   🥇 GOLD       │
                │ Business-ready  │
                │ Analytics       │
                └────────┬────────┘
                         ▲
                         │
                ┌─────────────────┐
                │ Apache Airflow  │
                │ Orchestration   │
                └─────────────────┘

📌 Overview
This project implements a modular data lake ETL pipeline using the Online Retail II dataset from the UCI Machine Learning Repository.
The pipeline processes 1,067,371 raw transaction records and transforms them into clean, partitioned, analytics-ready datasets.
Key capabilities
- ⚡ PySpark-based ETL processing
- 🏗️ Bronze / Silver / Gold data lake architecture
- 🧹 Data cleansing and deduplication
- ✅ Data quality validation
- 📦 Partitioned Parquet storage
- 🔎 Spark SQL analytics
- 📊 Customer, product, country and monthly analysis
- 🔄 Apache Airflow DAG for pipeline orchestration
- 🌱 Git/GitHub version control
- ☁️ Architecture prepared for AWS S3 and Athena deployment


🛠️ Tech Stack
Technology	Usage
Python	Pipeline development
PySpark	ETL and distributed transformations
Spark SQL	Analytical processing
Pandas	Data ingestion
PyArrow	Parquet processing
Parquet	Columnar data storage
Apache Airflow	Pipeline orchestration
Git	Version control
GitHub	Source control


📊 Dataset
Online Retail II — UCI Machine Learning Repository
The dataset contains e-commerce transactions including:
- Invoice
- Product / Stock Code
- Product Description
- Quantity
- Price
- Invoice Date
- Customer ID
- Country
Raw records processed: 1,067,371
Dataset source:
https://archive.ics.uci.edu/dataset/502/online+retail+ii


🥉 Bronze Layer
The ingestion layer reads the original Excel dataset and converts each yearly sheet into Parquet.
Process
Excel
  ↓
Pandas
  ↓
Schema normalization
  ↓
Parquet
  ↓
Bronze Layer

Output:
data/bronze/
├── year_2009_2010.parquet
└── year_2010_2011.parquet

Records ingested: 1,067,371

🥈 Silver Layer
The Silver layer performs the core PySpark ETL operations.
Transformations
- Schema normalization
- Data type conversion
- Invalid transaction filtering
- Cancellation / return handling
- Duplicate removal
- Revenue calculation
- Data quality processing
- Year/month partitioning
Storage
data/silver/sales/
├── year=2009/
│   └── month=12/
├── year=2010/
│   ├── month=1/
│   ├── month=2/
│   └── ...
└── year=2011/
    ├── month=1/
    └── ...

Final clean records: 1,007,913

🥇 Gold Layer
The Gold layer uses Spark SQL to create business-ready analytical datasets.
data/gold/
├── monthly_sales.parquet
├── country_sales.parquet
├── product_sales.parquet
└── customer_sales.parquet

Dataset	Records
Monthly Sales	25
Country Sales	43
Product Sales	4,917
Customer Sales	5,878


Analytics generated
Monthly Sales
- Revenue trends over time
Country Sales
- Revenue by country
Product Sales
- Product-level performance
Customer Sales
- Customer-level revenue analysis

🔄 Pipeline Orchestration
Apache Airflow DAG:
dags/ecommerce_data_lake_dag.py

Pipeline workflow:
Ingestion
    ↓
Silver Transformation
    ↓
Gold Analytics

The DAG executes the pipeline stages sequentially and is designed for scheduled ETL execution.
✅ Data Quality & Validation
The project includes automated validation for the generated datasets.
Validation covers:
- Record counts
- Duplicate detection
- Required fields
- Quantity validation
- Price validation
- Revenue calculation
- Gold dataset availability
Verification:
python tests/verify_pipeline.py

Latest verification
✓ monthly_sales: 25 rows
✓ country_sales: 43 rows
✓ product_sales: 4,917 rows
✓ customer_sales: 5,878 rows

📁 Project Structure
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

▶️ Run Locally
1. Clone
git clone https://github.com/jemmiii/cloud-ecommerce-data-lake.git
cd cloud-ecommerce-data-lake

2. Create environment
python -m venv venv

3. Activate
Windows:
.\venv\Scripts\Activate.ps1

4. Install dependencies
pip install -r requirements.txt

5. Run ingestion
python src/ingestion/ingest_raw.py

6. Run Silver transformation
python src/transformation/silver_transform.py

7. Generate Gold analytics
python src/analytics/gold_analytics.py

8. Verify pipeline
python tests/verify_pipeline.py

☁️ Cloud Deployment Architecture
The pipeline is designed to extend to AWS:
Local / Scheduled ETL
        │
        ▼
   PySpark Pipeline
        │
        ▼
      AWS S3
        │
   ┌────┼────┐
   ▼    ▼    ▼
Bronze Silver Gold
        │
        ▼
   Amazon Athena
        │
        ▼
   SQL Analytics

Planned cloud enhancements:
- AWS S3 data lake storage
- Amazon Athena analytics
- AWS Glue Data Catalog
- Cloud-based Airflow
- Incremental processing
- Pipeline monitoring
- CI/CD with GitHub Actions

💡 Engineering Highlights
- Processed 1M+ e-commerce records
- Built a modular ETL pipeline using PySpark
- Implemented Medallion Architecture
- Used partitioned Parquet for scalable storage
- Applied data cleansing and deduplication
- Built Spark SQL analytical workloads
- Added automated validation
- Added Airflow orchestration
- Maintained the project using Git/GitHub
- Designed the architecture for AWS cloud deployment


👨‍💻 Author
Jemin Patidar
B.Tech — Information Technology
Manipal University Jaipur
🔗 GitHub: https://github.com/jemmiii
🔗 Portfolio: https://jeminpatidar-portfolio.vercel.app/
