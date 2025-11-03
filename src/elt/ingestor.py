import os
from pathlib import Path
import duckdb
import pandas as pd
import kagglehub
import shutil 

# --- Path setup (works no matter where you run from) ---
REPO_ROOT = Path(__file__).resolve().parents[2]   # .../ae-ecommerce-portfolio/
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "olist.duckdb"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SCHEMA = "raw"

local_dir_path  = Path(kagglehub.dataset_download("olistbr/brazilian-ecommerce"))

print(f"[KAGGLEHUB] Pasta retornada: {local_dir_path}")
a = local_dir_path.resolve().parents[2]
print(f"a:{a}")

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

# --- DB Connection ---
con = duckdb.connect(DB_PATH)
con.execute(f"create schema if not exists {SCHEMA}")

# 5) Loop para criar/atualizar as tabelas a partir dos CSVs
for csv_name, table_name in FILES.items():
    csv_path = local_dir_path / csv_name
    if not csv_path.exists():
        print(f"[AVISO] Arquivo não encontrado: {csv_path}")
        continue

    # CREATE OR REPLACE TABLE garante idempotência
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {SCHEMA}.{table_name} AS
        SELECT *
        FROM read_csv_auto(?, filename=true, ignore_errors=true);
        """,
        [str(csv_path)],
    )
    # csv_path.unlink()

con.close()

shutil.rmtree(local_dir_path.resolve().parents[2])