from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "sales"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def parquet_rows(path):
    files = list(path.rglob("*.parquet"))

    total = 0

    for file in files:
        total += pq.read_metadata(file).num_rows

    return total, files


def verify():

    print("=" * 70)
    print("CLOUD E-COMMERCE DATA LAKE")
    print("END-TO-END PIPELINE VERIFICATION")
    print("=" * 70)

    # ---------------------------------------------------------
    # BRONZE
    # ---------------------------------------------------------

    print("\n[1] BRONZE LAYER")

    bronze_files = list(BRONZE_DIR.glob("*.parquet"))

    assert bronze_files, "Bronze Parquet files not found."

    bronze_rows = 0

    for file in bronze_files:
        rows = pq.read_metadata(file).num_rows
        bronze_rows += rows
        print(f"  ✓ {file.name}: {rows:,} rows")

    print(f"  Bronze total: {bronze_rows:,}")

    assert bronze_rows > 0

    # ---------------------------------------------------------
    # SILVER
    # ---------------------------------------------------------

    print("\n[2] SILVER LAYER")

    silver_rows, silver_files = parquet_rows(SILVER_DIR)

    assert silver_files, "Silver Parquet files not found."

    print(f"  Silver files: {len(silver_files)}")
    print(f"  Silver total: {silver_rows:,} rows")

    assert silver_rows > 0
    assert silver_rows <= bronze_rows

    # ---------------------------------------------------------
    # GOLD
    # ---------------------------------------------------------

    print("\n[3] GOLD LAYER")

    gold_datasets = [
    "monthly_sales",
    "country_sales",
    "product_sales",
    "customer_sales",
]
gold_datasets = [
    "monthly_sales",
    "country_sales",
    "product_sales",
    "customer_sales",
]
for dataset in gold_datasets:

    path = GOLD_DIR / f"{dataset}.parquet"

    assert path.exists(), f"Missing Gold dataset: {dataset}"

    rows = pq.read_metadata(path).num_rows

    print(
        f"  ✓ {dataset}: "
        f"{rows:,} rows"
    )

    assert rows > 0

  