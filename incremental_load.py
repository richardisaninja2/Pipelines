"""Incremental load pipeline: CSV -> cleaned dataframes -> Postgres (MERGE upsert).

Steps:
  1. Connect to the database
  2. Read the watermark (max updated_at in orders)
  3. Clean the CSV into orders (fact), customers and products (dimensions)
  4. Load to temp tables, verify row counts, then MERGE into production tables
"""
import os
import re
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "orders_20261025.csv"
ERROR_LOG = "error_log.txt"


def log_error(message: str) -> None:
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"{message}\n")


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #
def get_engine():
    load_dotenv()
    # Force the explicit synchronous psycopg2 driver
    db_url = re.sub(r"^postgresql:", "postgresql+psycopg2:", os.getenv("DATABASE_URL", ""))
    return create_engine(
        db_url,
        pool_recycle=300,   # cycle out stale connections
        pool_pre_ping=True,  # check connection health before executing
    )


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


def write_flagged(flagged_orders: pd.DataFrame) -> None:
    if not flagged_orders.empty:
        run_date = datetime.now().strftime("%Y%m%d")
        flagged_orders.to_csv(f"flagged_orders_{run_date}.csv", index=False)


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
                    count_col: str, merge_query, label: str) -> bool:
    """Load df into a temp table, verify row counts match, then MERGE and drop temp.

    Returns True if merged, False if skipped because counts did not match.
    """
    df.to_sql(temp_table, con=engine, if_exists="replace", index=False)

    count_df = pd.read_sql(f"SELECT count({count_col}) FROM {temp_table}", con=engine)
    local_count = len(df)
    db_count = int(count_df.iloc[0, 0])
    print(f"{label}: local={local_count} staged={db_count}")

    if local_count != db_count:
        log_error(
            f"Failed to execute merge query for {label} on {datetime.now()} "
            f"because local count ({local_count}) and sql table count ({db_count}) were off"
        )
        print(f"Mismatch with {label} counts - merge skipped, see {ERROR_LOG}")
        return False

    with engine.begin() as connection:
        connection.execute(merge_query)
        connection.execute(text(f"DROP TABLE {temp_table};"))
    return True


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    engine = get_engine()

    # sanity check connection
    print(pd.read_sql("SELECT * FROM orders LIMIT 5;", con=engine))

    date_to_load_from = get_watermark(engine)
    print(f"Watermark: {date_to_load_from}")

    df = pd.read_csv(CSV_PATH)

    valid_orders, flagged_orders = build_orders(df, date_to_load_from)
    write_flagged(flagged_orders)

    customers = build_customers(df)  # built but not loaded yet (same as notebook)
    products = build_products(df)

    stage_and_merge(engine, valid_orders, "orders_temp", "order_id", ORDERS_MERGE, "orders")
    stage_and_merge(engine, products, "products_temp", "product", PRODUCTS_MERGE, "products")


if __name__ == "__main__":
    main()
