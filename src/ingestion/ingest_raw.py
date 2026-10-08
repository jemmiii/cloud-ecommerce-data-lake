from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def normalize_raw_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize columns before writing the Bronze layer.

    Invoice is explicitly stored as string because the source
    contains both numeric invoice IDs and cancellation IDs such
    as C489449.
    """

    df = df.copy()

    # Keep business identifiers as strings.
    df["Invoice"] = df["Invoice"].astype("string")
    df["StockCode"] = df["StockCode"].astype("string")
    df["Description"] = df["Description"].astype("string")
    df["Country"] = df["Country"].astype("string")

    # Preserve numeric fields.
    df["Quantity"] = pd.to_numeric(
        df["Quantity"],
        errors="coerce"
    )

    df["Price"] = pd.to_numeric(
        df["Price"],
        errors="coerce"
    )

    # Preserve date/time as datetime.
    df["InvoiceDate"] = pd.to_datetime(
    df["InvoiceDate"],
    errors="coerce"
).astype("datetime64[us]")

    # Customer IDs may contain missing values, so use nullable string.
    df["Customer ID"] = df["Customer ID"].astype("string")

    return df


def ingest_raw_data():
    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {RAW_FILE}"
        )

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print("Reading raw Excel dataset...")
    print(f"Source: {RAW_FILE}")

    sheets = pd.read_excel(
        RAW_FILE,
        sheet_name=None
    )

    total_rows = 0

    for sheet_name, df in sheets.items():

        print(f"\nProcessing sheet: {sheet_name}")

        df = normalize_raw_schema(df)

        safe_name = (
            str(sheet_name)
            .lower()
            .replace("-", "_")
            .replace(" ", "_")
        )

        output_path = BRONZE_DIR / f"{safe_name}.parquet"

        df.to_parquet(
            output_path,
            engine="pyarrow",
            index=False
        )

        print(
            f"[BRONZE] {sheet_name}: "
            f"{len(df):,} rows → {output_path.name}"
        )

        total_rows += len(df)

    print("\n" + "-" * 60)
    print(f"Total rows ingested: {total_rows:,}")
    print(f"Bronze location: {BRONZE_DIR}")
    print("Bronze ingestion completed successfully.")


if __name__ == "__main__":
    ingest_raw_data()