from ETL.Connection import get_dwh_conn
from datetime import datetime
import pandas as pd
from io import StringIO


# ---------------- LOG FUNCTION ----------------

def log_etl(cursor, pipeline, layer, start, end, status, rows=0, error=None):

    cursor.execute("""
        INSERT INTO gold.etl_logs (
            pipeline_name,
            layer,
            start_time,
            end_time,
            status,
            rows_loaded,
            error_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        pipeline,
        layer,
        start,
        end,
        status,
        rows,
        error
    ))


# ---------------- GOLD ETL ----------------

def run_gold_load():

    conn = get_dwh_conn()
    cursor = conn.cursor()

    start_time = datetime.now()

    try:

        # ---------------- CLEAR GOLD ----------------

        cursor.execute("""
            TRUNCATE TABLE
                gold.fact_sales,
                gold.dim_customer,
                gold.dim_product,
                gold.dim_date,
                gold.dim_lead
        """)

        # ---------------- EXTRACT ----------------

        customers = pd.read_sql(
            "SELECT * FROM silver.customers_clean",
            conn
        )

        products = pd.read_sql(
            "SELECT * FROM silver.products_clean",
            conn
        )

        leads = pd.read_sql(
            "SELECT * FROM silver.leads_clean",
            conn
        )

        orders = pd.read_sql("""
            SELECT order_id, customer_id, order_date
            FROM silver.orders_clean
        """, conn)

        order_items = pd.read_sql("""
            SELECT *
            FROM silver.order_items_clean
        """, conn)

        # ---------------- FACT SALES ----------------

        fact_sales = pd.merge(order_items, orders, on="order_id", how="left")

        fact_sales["total_amount"] = fact_sales["quantity"] * fact_sales["price"]

        fact_sales = fact_sales[[
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "price",
            "total_amount"
        ]]

        # ---------------- DIM DATE ----------------

        orders["order_date"] = pd.to_datetime(orders["order_date"])

        dim_date = orders[["order_date"]].drop_duplicates()

        dim_date["date_id"] = dim_date["order_date"].dt.date
        dim_date["year"] = dim_date["order_date"].dt.year
        dim_date["month"] = dim_date["order_date"].dt.month
        dim_date["day"] = dim_date["order_date"].dt.day
        dim_date["week"] = dim_date["order_date"].dt.isocalendar().week

        dim_date = dim_date[[
            "date_id",
            "year",
            "month",
            "day",
            "week"
        ]]

        # ---------------- LOAD FUNCTION ----------------

        def load(df, table):

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cursor.copy_expert(f"""
                COPY gold.{table}
                ({','.join(df.columns)})
                FROM STDIN WITH CSV
            """, buffer)

            return len(df)

        # ---------------- LOAD DIMENSIONS ----------------

        total_rows = 0

        total_rows += load(customers, "dim_customer")
        total_rows += load(products, "dim_product")
        total_rows += load(leads, "dim_lead")
        total_rows += load(dim_date, "dim_date")

        # ---------------- LOAD FACT ----------------

        total_rows += load(fact_sales, "fact_sales")

        conn.commit()

        end_time = datetime.now()

        # ---------------- SUCCESS LOG ----------------

        log_etl(
            cursor,
            "dwh_pipeline",
            "gold",
            start_time,
            end_time,
            "SUCCESS",
            total_rows,
            None
        )

        conn.commit()

        print("GOLD COMPLETE")

    except Exception as e:

        end_time = datetime.now()

        log_etl(
            cursor,
            "dwh_pipeline",
            "gold",
            start_time,
            end_time,
            "FAILED",
            0,
            str(e)
        )

        conn.rollback()
        raise

    finally:

        conn.close()
