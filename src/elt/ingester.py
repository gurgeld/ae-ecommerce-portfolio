from pathlib import Path
import duckdb
import pandas as pd
from kagglehub.dataset import KaggleDatasetAdapter
from kagglehub.dataset import download as dataset_download

# --- Files ---
FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}

# --- Path setup (works no matter where you run from) ---
REPO_ROOT = Path(__file__).resolve().parents[2]   # .../ae-ecommerce-portfolio/
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "olist.duckdb"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SCHEMA = "raw"

# --- Dataset download ---
DATASET_HANDLE = "olistbr/brazilian-ecommerce"
DATASET = dataset_download(DATASET_HANDLE, adapter=KaggleDatasetAdapter.PANDAS)

# --- DB Connection ---
con = duckdb.connect(DB_PATH)
con.execute(f"create schema if not exists {SCHEMA}")

# --- Fix encoding ---
def load_csv_with_fallback(file_path: str) -> pd.DataFrame:
    """Load a CSV file from Kaggle using the in-memory pandas adapter."""

    try:
        df = DATASET[file_path]
    except KeyError as exc:
        raise FileNotFoundError(
            f"Could not find {file_path} in Kaggle dataset {DATASET_HANDLE}"
        ) from exc

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"Expected pandas DataFrame for {file_path}, got {type(df)!r} from KaggleHub"
        )

    # Copy so we can add metadata columns without mutating the cached DataFrame
    return df.copy()


for file_path, table in FILES.items():
    df = load_csv_with_fallback(file_path)
    df["_ingested_at"] = pd.Timestamp.utcnow()
    df["_source_file"] = file_path

    con.register("df", df)
    con.execute(f'create or replace table {SCHEMA}."{table}" as select * from df')
    rows = con.execute(f'select count(*) from {SCHEMA}."{table}"').fetchone()[0]
    print(f"Loaded {table}: {rows} rows")

con.close()
