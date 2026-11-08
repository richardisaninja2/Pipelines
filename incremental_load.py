"""Incremental load pipeline: CSV -> cleaned dataframes -> Postgres (MERGE upsert).

Steps:
  1. Connect to the database
  2. Read the watermark (max updated_at in orders)
  3. Clean the CSV into orders (fact), customers and products (dimensions)
  4. Load to temp tables, verify row counts, then MERGE into production tables

Can be used as a library (run_pipeline) from the FastAPI app, or from the CLI:
    python incremental_load.py orders_20261025.csv
"""
import logging
import os
import re
import sys
import tempfile
from datetime import datetime
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logger = logging.getLogger("incremental_load")

# Cloud Run's filesystem is in-memory and ephemeral; only /tmp is writable.
OUTPUT_DIR = os.getenv("OUTPUT_DIR", tempfile.gettempdir())


def log_error(message: str) -> None:
    logger.error(message)
    with open(os.path.join(OUTPUT_DIR, "error_log.txt"), "a", encoding="utf-8") as f:
        f.write(f"{message}\n")


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #
_engine = None


def get_engine():
    """Create the engine once and reuse it across requests."""
    global _engine
    if _engine is None:
        load_dotenv()
        # Force the explicit synchronous psycopg2 driver
        db_url = re.sub(r"^postgres(ql)?:", "postgresql+psycopg2:", os.getenv("DATABASE_URL", ""))
        _engine = create_engine(
            db_url,
            pool_recycle=300,    # cycle out stale connections
            pool_pre_ping=True,  # check connection health before executing
        )
    return _engine


def get_watermark(engine) -> str:
    """Latest updated_at already loaded into the orders table."""
    result = pd.read_sql("select max(updated_at) from orders", con=engine)
    value = pd.to_datetime(result.iloc[0, 0])
    return value.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- #
# Transform
# --------------------------------------------------------------------------- #
def build_orders(df: pd.DataFrame, date_to_load_from: str):
    """Return (valid_orders, flagged_orders) for rows newer than the watermark."""
    orders = df[
        ["order_id", "customer_id", "order_date", "product",
         "total_amount", "status", "country", "updated_at"]
    ].copy()

    # only rows after the cutoff
    cutoff_time = pd.to_datetime(date_to_load_from)
    orders["updated_at"] = pd.to_datetime(orders["updated_at"])
    orders = orders[orders["updated_at"] > cutoff_time].copy()

    # drop duplicates, keep the last entry
    orders = orders.drop_duplicates(subset="order_id", keep="last")

    # flag orders with no total amount
    flagged_orders = orders[orders["total_amount"].isna()].copy()

    # single date format, upper-case country
    orders["order_date"] = pd.to_datetime(orders["order_date"], format="mixed")
    orders["country"] = orders["country"].str.upper()

    # valid orders = valid total amount (change based on business needs)
    valid_orders = orders[orders["total_amount"].notna()].copy()
    valid_orders["status"] = valid_orders["status"].str.lower()
    valid_orders["total_amount"] = (
        valid_orders["total_amount"].astype(str).str.replace("$", "", regex=False).astype(float)
    )
    return valid_orders, flagged_orders


def parse_source_name(source_name: str | None) -> dict[str, Any]:
    """Pull the file name, stem and embedded date (YYYYMMDD) out of a path or gs:// URI.

    'gs://my-bucket/incoming/orders_20261025.csv' ->
        {'file_name': 'orders_20261025.csv', 'file_stem': 'orders_20261025', 'file_date': '2026-10-25'}
    """
    if not source_name:
        return {"file_name": None, "file_stem": None, "file_date": None}

    file_name = os.path.basename(source_name)
    file_stem = os.path.splitext(file_name)[0]

    file_date = None
    match = re.search(r"(\d{8})", file_stem)
    if match:
        try:
            file_date = datetime.strptime(match.group(1), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass  # 8 digits but not a real date

    return {"file_name": file_name, "file_stem": file_stem, "file_date": file_date}


def write_flagged(flagged_orders: pd.DataFrame, file_stem: str | None = None) -> str | None:
    """Write flagged rows to a CSV; returns the path (or None if nothing flagged).

    Named after the source file (flagged_orders_20261025.csv) when known,
    otherwise after today's date.
    """
    if flagged_orders.empty:
        return None
    suffix = file_stem or f"orders_{datetime.now().strftime('%Y%m%d')}"
    path = os.path.join(OUTPUT_DIR, f"flagged_{suffix}.csv")
    flagged_orders.to_csv(path, index=False)
    return path


def build_customers(df: pd.DataFrame) -> pd.DataFrame:
    customers = df[["customer_id", "customer_name", "email", "country"]].copy()
    customers["customer_name"] = customers["customer_name"].str.title().fillna("Missing")
    customers = customers.drop_duplicates(subset="customer_id", keep="first")
    customers["country"] = customers["country"].str.upper().fillna("Missing")
    customers["email"] = customers["email"].fillna("Missing")
    return customers


def build_products(df: pd.DataFrame) -> pd.DataFrame:
    products = df[["product", "category", "unit_price"]].copy()
    products["unit_price"] = products["unit_price"].astype(str).str.replace("$", "", regex=False).astype(float)
    products = products.drop_duplicates(subset="product")
    products["category"] = products["category"].str.title()
    products["product"] = products["product"].str.title()
    return products


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #
ORDERS_MERGE = text("""
    MERGE INTO orders AS target
    USING orders_temp AS source
    ON target.order_id = source.order_id::text
    WHEN MATCHED THEN
        UPDATE SET
            total_amount = source.total_amount::double precision,
            status       = source.status,
            updated_at   = source.updated_at::timestamp,
            order_date   = source.order_date::date,
            product      = source.product,
            country      = source.country
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_id, order_date, product, status, country, total_amount, updated_at)
        VALUES (
            source.order_id::text,
            source.customer_id::integer,
            source.order_date::date,
            source.product,
            source.status,
            source.country,
            source.total_amount::double precision,
            source.updated_at::timestamp
        );
""")

PRODUCTS_MERGE = text("""
    MERGE INTO products AS target
    USING products_temp AS source
    ON target.product = source.product
    WHEN MATCHED THEN
        UPDATE SET
            category   = source.category,
            unit_price = source.unit_price
    WHEN NOT MATCHED THEN
        INSERT (product, category, unit_price)
        VALUES (source.product, source.category, source.unit_price)
""")


def stage_and_merge(engine, df: pd.DataFrame, temp_table: str,
                    count_col: str, merge_query, label: str) -> dict[str, Any]:
    """Load df into a temp table, verify row counts match, then MERGE and drop temp."""
    df.to_sql(temp_table, con=engine, if_exists="replace", index=False)

    count_df = pd.read_sql(f"SELECT count({count_col}) FROM {temp_table}", con=engine)
    local_count = len(df)
    db_count = int(count_df.iloc[0, 0])
    logger.info("%s: local=%s staged=%s", label, local_count, db_count)

    if local_count != db_count:
        log_error(
            f"Failed to execute merge query for {label} on {datetime.now()} "
            f"because local count ({local_count}) and sql table count ({db_count}) were off"
        )
        return {"merged": False, "local_count": local_count, "staged_count": db_count}

    with engine.begin() as connection:
        connection.execute(merge_query)
        connection.execute(text(f"DROP TABLE {temp_table};"))
    return {"merged": True, "local_count": local_count, "staged_count": db_count}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run_pipeline(source, source_name: str | None = None) -> dict[str, Any]:
    """Run the full incremental load.

    `source` is anything pandas.read_csv accepts: a path or a file-like object.
    `source_name` is the file's identity (e.g. 'gs://bucket/incoming/orders_20261025.csv');
    when omitted and `source` is a path string, that path is used.
    Returns a JSON-serialisable summary of what happened.
    """
    if source_name is None and isinstance(source, (str, os.PathLike)):
        source_name = str(source)
    file_info = parse_source_name(source_name)
    logger.info("Starting load for %s", source_name or "<unnamed source>")

    engine = get_engine()

    date_to_load_from = get_watermark(engine)
    logger.info("Watermark before load: %s", date_to_load_from)

    df = pd.read_csv(source)
    rows_in_file = len(df)

    valid_orders, flagged_orders = build_orders(df, date_to_load_from)
    flagged_path = write_flagged(flagged_orders, file_info["file_stem"])

    customers = build_customers(df)  # built but not loaded yet (same as notebook)
    products = build_products(df)

    orders_result = stage_and_merge(engine, valid_orders, "orders_temp", "order_id", ORDERS_MERGE, "orders")
    products_result = stage_and_merge(engine, products, "products_temp", "product", PRODUCTS_MERGE, "products")

    new_watermark = (
        valid_orders["updated_at"].max().strftime("%Y-%m-%d %H:%M:%S")
        if not valid_orders.empty else date_to_load_from
    )

    return {
        "source": source_name,
        "file_name": file_info["file_name"],
        "file_date": file_info["file_date"],
        "watermark": date_to_load_from,
        "new_watermark": new_watermark,  # what the watermark becomes once the merge succeeds
        "rows_in_file": rows_in_file,
        "new_valid_orders": len(valid_orders),
        "flagged_orders": len(flagged_orders),
        "flagged_file": flagged_path,
        "customers_built": len(customers),
        "orders": orders_result,
        "products": products_result,
        "success": orders_result["merged"] and products_result["merged"],
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "orders_20261025.csv"
    print(run_pipeline(csv_path))
