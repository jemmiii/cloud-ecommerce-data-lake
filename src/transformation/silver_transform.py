from pathlib import Path
import shutil

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CloudEcommerce-SilverTransformation")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def transform_to_silver():

    spark = create_spark_session()

    print("=" * 70)
    print("CLOUD E-COMMERCE DATA LAKE")
    print("BRONZE -> SILVER TRANSFORMATION")
    print("=" * 70)

    try:

        # ---------------------------------------------------------
        # 1. READ BRONZE
        # ---------------------------------------------------------

        print("\n[1/8] Reading Bronze datasets...")

        bronze_files = [
            str(BRONZE_DIR / "year_2009_2010.parquet"),
            str(BRONZE_DIR / "year_2010_2011.parquet"),
        ]

        df = spark.read.parquet(*bronze_files)

        initial_count = df.count()

        print(f"Initial records: {initial_count:,}")

        # ---------------------------------------------------------
        # 2. STANDARDIZE COLUMN NAMES
        # ---------------------------------------------------------

        print("\n[2/8] Standardizing schema...")

        df = (
            df
            .withColumnRenamed("Invoice", "invoice_id")
            .withColumnRenamed("StockCode", "stock_code")
            .withColumnRenamed("Description", "description")
            .withColumnRenamed("Quantity", "quantity")
            .withColumnRenamed("InvoiceDate", "invoice_date")
            .withColumnRenamed("Price", "unit_price")
            .withColumnRenamed("Customer ID", "customer_id")
            .withColumnRenamed("Country", "country")
        )

        # ---------------------------------------------------------
        # 3. ENFORCE TYPES
        # ---------------------------------------------------------

        print("\n[3/8] Enforcing data types...")

        df = (
            df
            .withColumn("invoice_id", F.col("invoice_id").cast(StringType()))
            .withColumn("stock_code", F.col("stock_code").cast(StringType()))
            .withColumn("description", F.col("description").cast(StringType()))
            .withColumn("quantity", F.col("quantity").cast(IntegerType()))
            .withColumn(
                "invoice_date",
                F.col("invoice_date").cast(TimestampType())
            )
            .withColumn(
                "unit_price",
                F.col("unit_price").cast(DoubleType())
            )
            .withColumn(
                "customer_id",
                F.col("customer_id").cast(StringType())
            )
            .withColumn("country", F.col("country").cast(StringType()))
        )

        # ---------------------------------------------------------
        # 4. IDENTIFY CANCELLATIONS
        # ---------------------------------------------------------

        print("\n[4/8] Identifying cancelled transactions...")

        df = df.withColumn(
            "is_cancelled",
            F.when(
                F.upper(F.col("invoice_id")).startswith("C"),
                F.lit(True)
            ).otherwise(F.lit(False))
        )

        cancellation_count = (
            df.filter(F.col("is_cancelled") == True)
            .count()
        )

        print(
            f"Cancelled transactions detected: "
            f"{cancellation_count:,}"
        )

        # ---------------------------------------------------------
        # 5. DATA QUALITY
        # ---------------------------------------------------------

        print("\n[5/8] Applying data quality rules...")

        before_quality = df.count()

        df = (
            df
            .filter(F.col("invoice_id").isNotNull())
            .filter(F.col("stock_code").isNotNull())
            .filter(F.col("invoice_date").isNotNull())
            .filter(F.col("quantity").isNotNull())
            .filter(F.col("unit_price").isNotNull())
            .filter(F.col("quantity") > 0)
            .filter(F.col("unit_price") > 0)
            .filter(F.col("is_cancelled") == False)
        )

        after_quality = df.count()

        print(
            f"Records before quality filtering: "
            f"{before_quality:,}"
        )

        print(
            f"Clean sales records: "
            f"{after_quality:,}"
        )

        print(
            f"Records removed: "
            f"{before_quality - after_quality:,}"
        )

        # ---------------------------------------------------------
        # 6. DEDUPLICATION
        # ---------------------------------------------------------

        print("\n[6/8] Deduplicating and creating metrics...")

        before_dedup = df.count()

        df = df.dropDuplicates()

        after_dedup = df.count()

        print(
            f"Duplicate records removed: "
            f"{before_dedup - after_dedup:,}"
        )

        # Revenue
        df = df.withColumn(
            "revenue",
            F.round(
                F.col("quantity") * F.col("unit_price"),
                2
            )
        )

        # Date dimensions
        df = (
            df
            .withColumn("year", F.year("invoice_date"))
            .withColumn("month", F.month("invoice_date"))
            .withColumn(
                "year_month",
                F.date_format("invoice_date", "yyyy-MM")
            )
        )

        # ---------------------------------------------------------
        # 7. COLLECT CLEAN DATA FROM SPARK
        # ---------------------------------------------------------

        print("\n[7/8] Preparing Silver output...")

        # Convert Spark DataFrame to Pandas only for the final
        # local Parquet write. Transformation itself remains Spark.
        pandas_df = df.toPandas()

        print(
            f"Rows prepared for Silver: "
            f"{len(pandas_df):,}"
        )

        # ---------------------------------------------------------
        # 8. WRITE PARTITIONED PARQUET WITH PYARROW
        # ---------------------------------------------------------

        print("\n[8/8] Writing partitioned Silver Parquet...")

        silver_path = SILVER_DIR / "sales"

        if silver_path.exists():
            shutil.rmtree(silver_path)

        silver_path.mkdir(
            parents=True,
            exist_ok=True
        )

        pandas_df.to_parquet(
            silver_path,
            engine="pyarrow",
            partition_cols=["year", "month"],
            index=False
        )

        print("\n" + "=" * 70)
        print("SILVER TRANSFORMATION COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(f"Initial records:      {initial_count:,}")
        print(f"Clean sales:          {after_quality:,}")
        print(f"After deduplication:  {after_dedup:,}")
        print(f"Silver output:        {silver_path}")

        print("\nFinal Silver schema:")
        df.printSchema()

        print("\nSample Silver records:")

        df.select(
            "invoice_id",
            "stock_code",
            "description",
            "quantity",
            "unit_price",
            "revenue",
            "country",
            "year_month"
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    transform_to_silver()

    