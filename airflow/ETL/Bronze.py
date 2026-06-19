from ETL.Connection import get_erp_conn, get_crm_conn, get_dwh_conn
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


# ---------------- BRONZE ETL ----------------

def run_bronze_load():

    erp_conn = get_erp_conn()
    crm_conn = get_crm_conn()
    dwh_conn = get_dwh_conn()

    cursor = dwh_conn.cursor()

    start_time = datetime.now()

    try:

        # ---------------- CLEAR BRONZE ----------------

        cursor.execute("""
            TRUNCATE TABLE
                bronze.erp_orders,
                bronze.erp_products,
                bronze.erp_order_items,
                bronze.crm_customers,
                bronze.crm_leads
        """)

        # ---------------- EXTRACT ----------------

        orders = pd.read_sql("SELECT * FROM orders", erp_conn)
        products = pd.read_sql("SELECT * FROM products", erp_conn)
        order_items = pd.read_sql("SELECT * FROM order_items", erp_conn)

        customers = pd.read_sql("SELECT * FROM public.customers", crm_conn)
        leads = pd.read_sql("SELECT * FROM public.leads", crm_conn)

        # ---------------- LOAD FUNCTION ----------------

        def load(df, table):

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cursor.copy_expert(f"""
                COPY bronze.{table}
                ({','.join(df.columns)})
                FROM STDIN WITH CSV
            """, buffer)

            return len(df)

        # ---------------- LOAD DATA ----------------

        total_rows = 0

        total_rows += load(orders, "erp_orders")
        total_rows += load(products, "erp_products")
        total_rows += load(order_items, "erp_order_items")

        total_rows += load(customers, "crm_customers")
        total_rows += load(leads, "crm_leads")

        dwh_conn.commit()

        end_time = datetime.now()

        # ---------------- SUCCESS LOG ----------------

        log_etl(
            cursor,
            "dwh_pipeline",
            "bronze",
            start_time,
            end_time,
            "SUCCESS",
            total_rows,
            None
        )

        dwh_conn.commit()

        print("BRONZE LOAD COMPLETE")

    except Exception as e:

        end_time = datetime.now()

        log_etl(
            cursor,
            "dwh_pipeline",
            "bronze",
            start_time,
            end_time,
            "FAILED",
            0,
            str(e)
        )

        dwh_conn.rollback()
        raise

    finally:

        erp_conn.close()
        crm_conn.close()
        dwh_conn.close()