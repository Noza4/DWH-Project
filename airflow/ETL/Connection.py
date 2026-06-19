import psycopg2


def get_erp_conn():
    return psycopg2.connect(
        host="dwh-erp.cfuy0auq4ttj.eu-north-1.rds.amazonaws.com",
        dbname="erp",
        user="erp_admin",
        password="Nikolozi2004$",
        port=5432
    )


def get_crm_conn():
    return psycopg2.connect(
        host="dwh-crm.cfuy0auq4ttj.eu-north-1.rds.amazonaws.com",
        dbname="CRM",
        user="crm_admin",
        password="Nikolozi2004$",
        port=5432
    )


def get_dwh_conn():
    return psycopg2.connect(
        host="dwh.cfuy0auq4ttj.eu-north-1.rds.amazonaws.com",
        dbname="dwh",
        user="dwh_admin",
        password="Nikolozi2004$",
        port=5432
    )