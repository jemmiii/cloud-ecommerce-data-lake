from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "sales"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("CloudEcommerce-GoldAnalytics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def read_silver_with_pyarrow():

    print("\nReading Silver Parquet using PyArrow...")

    parquet_files = list(SILVER_PATH.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found inside: {SILVER_PATH}"
        )

    print(f"Silver Parquet files found: {len(parquet_files)}")

    dataframes = []

    for file_path in parquet_files:

        print(f"  Reading: {file_path.name}")

        table = pq.read_table(file_path)
        df = table.to_pandas()

        dataframes.append(df)

    silver_df = pd.concat(
        dataframes,
        ignore_index=True
    )

    print(
        f"Silver records loaded: "
        f"{len(silver_df):,}"
    )

    return silver_df


def write_gold(df, output_file):

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if output_file.exists():
        output_file.unlink()

    df.to_parquet(
        output_file,
        engine="pyarrow",
        index=False
    )

    print(f"  Written: {output_file}")


def create_gold_layer():

    spark = create_spark_session()

    print("=" * 70)
    print("CLOUD E-COMMERCE DATA LAKE")
    print("SILVER -> GOLD ANALYTICS")
    print("=" * 70)

    try:

        # ---------------------------------------------------------
        # 1. READ SILVER
        # ---------------------------------------------------------

        print("\n[1/6] Reading Silver data...")

        pandas_df = read_silver_with_pyarrow()

        # ---------------------------------------------------------
        # 2. CREATE SPARK DATAFRAME
        # ---------------------------------------------------------

        print("\n[2/6] Creating Spark DataFrame...")

        df = spark.createDataFrame(pandas_df)

        total_records = df.count()

        print(
            f"Spark DataFrame records: "
            f"{total_records:,}"
        )

        df.createOrReplaceTempView("sales")

        # ---------------------------------------------------------
        # 3. MONTHLY SALES
        # ---------------------------------------------------------

        print("\n[3/6] Creating monthly sales dataset...")

        monthly_sales = spark.sql("""
            SELECT
                year_month,
                COUNT(DISTINCT invoice_id) AS total_orders,
                COUNT(DISTINCT customer_id) AS unique_customers,
                SUM(quantity) AS total_units,
                ROUND(SUM(revenue), 2) AS total_revenue,
                ROUND(AVG(revenue), 2) AS average_order_value
            FROM sales
            GROUP BY year_month
            ORDER BY year_month
        """)

        print("\nMonthly Sales:")
        monthly_sales.show(20, truncate=False)

        # ---------------------------------------------------------
        # 4. COUNTRY SALES
        # ---------------------------------------------------------

        print("\n[4/6] Creating country sales dataset...")

        country_sales = spark.sql("""
            SELECT
                country,
                COUNT(DISTINCT invoice_id) AS total_orders,
                COUNT(DISTINCT customer_id) AS unique_customers,
                SUM(quantity) AS total_units,
                ROUND(SUM(revenue), 2) AS total_revenue
            FROM sales
            GROUP BY country
            ORDER BY total_revenue DESC
        """)

        print("\nTop Countries:")
        country_sales.show(15, truncate=False)

        # ---------------------------------------------------------
        # 5. PRODUCT + CUSTOMER SALES
        # ---------------------------------------------------------

        print("\n[5/6] Creating product and customer datasets...")

        product_sales = spark.sql("""
            SELECT
                stock_code,
                FIRST(description, true) AS product_description,
                SUM(quantity) AS units_sold,
                COUNT(DISTINCT invoice_id) AS total_orders,
                ROUND(SUM(revenue), 2) AS total_revenue,
                ROUND(AVG(unit_price), 2) AS average_unit_price
            FROM sales
            GROUP BY stock_code
            ORDER BY total_revenue DESC
        """)

        customer_sales = spark.sql("""
            SELECT
                customer_id,
                COUNT(DISTINCT invoice_id) AS total_orders,
                SUM(quantity) AS total_units,
                ROUND(SUM(revenue), 2) AS total_revenue,
                ROUND(AVG(revenue), 2) AS average_order_value,
                MIN(invoice_date) AS first_purchase,
                MAX(invoice_date) AS last_purchase
            FROM sales
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
            ORDER BY total_revenue DESC
        """)

        print("\nTop Products:")
        product_sales.show(15, truncate=False)

        print("\nTop Customers:")
        customer_sales.show(15, truncate=False)

        # ---------------------------------------------------------
        # 6. WRITE GOLD DATASETS
        # ---------------------------------------------------------

        print("\n[6/6] Writing Gold datasets...")

        monthly_path = GOLD_DIR / "monthly_sales.parquet"
        country_path = GOLD_DIR / "country_sales.parquet"
        product_path = GOLD_DIR / "product_sales.parquet"
        customer_path = GOLD_DIR / "customer_sales.parquet"

        write_gold(
            monthly_sales.toPandas(),
            monthly_path
        )

        write_gold(
            country_sales.toPandas(),
            country_path
        )

        write_gold(
            product_sales.toPandas(),
            product_path
        )

        write_gold(
            customer_sales.toPandas(),
            customer_path
        )

        print("\n" + "=" * 70)
        print("GOLD LAYER COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(f"\nGold location: {GOLD_DIR}")

        print("\nDatasets created:")
        print("  Monthly sales")
        print("  Country sales")
        print("  Product sales")
        print("  Customer sales")

    finally:

        spark.stop()


if __name__ == "__main__":
    create_gold_layer()