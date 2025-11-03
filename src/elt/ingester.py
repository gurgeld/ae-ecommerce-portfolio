from pathlib import Path
import duckdb
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter
from typing import Iterable

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

# --- DB Connection ---
con = duckdb.connect(DB_PATH)
con.execute(f"create schema if not exists {SCHEMA}")

# --- Fix encoding ---
def load_csv_with_fallback(
    handle: str,
    file_path: str,
    encodings: Iterable[str] = ("utf-8-sig", "utf-8", "cp1252", "latin1"),
) -> pd.DataFrame:
    """Load a CSV file from Kaggle trying multiple encodings/parse strategies."""

    base_kwargs = {
        "encoding_errors": "replace",
        "dtype": "string",
    }
    parse_strategies = (
        {},  # default fast "c" engine
        {"engine": "python", "on_bad_lines": "warn"},
        {"engine": "python", "on_bad_lines": "skip"},
    )

    last_err = None
    for enc in encodings:
        for extra_kwargs in parse_strategies:
            try:
                pandas_kwargs = {**base_kwargs, **extra_kwargs, "encoding": enc}
                return kagglehub.dataset_load(
                    KaggleDatasetAdapter.PANDAS,
                    handle,
                    file_path,
                    pandas_kwargs=pandas_kwargs,
                )
            except Exception as e:
                last_err = e
    # If none worked, bubble up the last error with context
    raise RuntimeError(
        f"Failed to read {file_path} with tried encodings: {encodings} and parse strategies: {parse_strategies}"
    ) from last_err


for file_path, table in FILES.items():
    df = load_csv_with_fallback("olistbr/brazilian-ecommerce", file_path)
    df["_ingested_at"] = pd.Timestamp.utcnow()
    df["_source_file"] = file_path

    con.register("df", df)
    con.execute(f'create or replace table {SCHEMA}."{table}" as select * from df')
    rows = con.execute(f'select count(*) from {SCHEMA}."{table}"').fetchone()[0]
    print(f"Loaded {table}: {rows} rows")

con.close()
