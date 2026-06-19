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


# ---------------- SILVER ETL ----------------

def run_silver_transform():

    conn = get_dwh_conn()
    cursor = conn.cursor()

    start_time = datetime.now()

    try:

        # ---------------- CLEAR SILVER ----------------

        cursor.execute("""
            TRUNCATE TABLE
                silver.orders_clean,
                silver.products_clean,
                silver.order_items_clean,
                silver.customers_clean,
                silver.leads_clean
        """)

        # ---------------- EXTRACT ----------------

        orders = pd.read_sql("SELECT * FROM bronze.erp_orders", conn)
        products = pd.read_sql("SELECT * FROM bronze.erp_products", conn)
        order_items = pd.read_sql("SELECT * FROM bronze.erp_order_items", conn)

        customers = pd.read_sql("SELECT * FROM bronze.crm_customers", conn)
        leads = pd.read_sql("SELECT * FROM bronze.crm_leads", conn)

        # ---------------- TRANSFORM ----------------

        def clean_text(df):
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
            return df

        orders = orders.drop_duplicates()
        products = products.drop_duplicates()
        order_items = order_items.drop_duplicates()

        customers = clean_text(customers).drop_duplicates()
        leads = clean_text(leads).drop_duplicates()

        # ---------------- LOAD FUNCTION ----------------

        def load(df, table):

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cursor.copy_expert(f"""
                COPY silver.{table}
                ({','.join(df.columns)})
                FROM STDIN WITH CSV
            """, buffer)

            return len(df)

        # ---------------- LOAD ----------------

        total_rows = 0

        total_rows += load(orders, "orders_clean")
        total_rows += load(products, "products_clean")
        total_rows += load(order_items, "order_items_clean")
        total_rows += load(customers, "customers_clean")
        total_rows += load(leads, "leads_clean")

        conn.commit()

        end_time = datetime.now()

        # ---------------- SUCCESS LOG ----------------

        log_etl(
            cursor,
            "dwh_pipeline",
            "silver",
            start_time,
            end_time,
            "SUCCESS",
            total_rows,
            None
        )

        conn.commit()

        print("SILVER COMPLETE")

    except Exception as e:

        end_time = datetime.now()

        log_etl(
            cursor,
            "dwh_pipeline",
            "silver",
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
