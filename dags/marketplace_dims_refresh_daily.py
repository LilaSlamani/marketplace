"""
DAG : marketplace_dims_refresh_daily
Charge les référentiels dans les tables de dimensions du DWH.

Ordre d'exécution (respecte les FK du schéma) :
    dim_date → dim_seller → dim_product
                          → dim_customer

Stratégie : UPSERT (INSERT ... ON CONFLICT DO UPDATE)
→ met à jour ce qui existe, insère ce qui est nouveau, ne supprime rien.
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, date, timedelta

from hooks.marketplace_hook import MarketplaceAPIHook


@dag(
    dag_id="marketplace_dims_refresh_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["marketplace", "dimensions"],
)
def marketplace_dims_refresh_daily():

    @task()
    def refresh_dim_date():
        """
        Peuple dwh.dim_date pour toute l'année 2026.
        ON CONFLICT DO NOTHING : idempotent, on peut rejouer sans doublon.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        sql = """
            INSERT INTO dwh.dim_date (dt, year, month, day_of_week, is_weekend)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dt) DO NOTHING
        """
        rows = []
        current = date(2026, 1, 1)
        end     = date(2026, 12, 31)
        while current <= end:
            rows.append((
                current,
                current.year,
                current.month,
                current.weekday(),       # 0 = lundi, 6 = dimanche
                current.weekday() >= 5,  # True si samedi ou dimanche
            ))
            current += timedelta(days=1)

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        print(f"dim_date : {len(rows)} dates chargées (2026-01-01 → 2026-12-31)")

    @task()
    def refresh_dim_seller():
        """Upsert des 60 vendeurs dans dwh.dim_seller."""
        api_hook = MarketplaceAPIHook()
        pg_hook  = PostgresHook(postgres_conn_id="postgres_dwh")

        sellers = api_hook.get_sellers()

        sql = """
            INSERT INTO dwh.dim_seller (seller_id, name, country, joined_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (seller_id) DO UPDATE SET
                name        = EXCLUDED.name,
                country     = EXCLUDED.country,
                joined_date = EXCLUDED.joined_date
        """
        rows = [
            (s["seller_id"], s["name"], s["country"], s["joined_date"])
            for s in sellers
        ]
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        print(f"dim_seller : {len(rows)} vendeurs upsertés")

    @task()
    def refresh_dim_product():
        """
        Upsert des 200 produits dans dwh.dim_product.
        Doit tourner après refresh_dim_seller (FK seller_id → dim_seller).
        """
        api_hook = MarketplaceAPIHook()
        pg_hook  = PostgresHook(postgres_conn_id="postgres_dwh")

        products = api_hook.get_products()

        sql = """
            INSERT INTO dwh.dim_product (product_id, name, category, base_price, seller_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                name       = EXCLUDED.name,
                category   = EXCLUDED.category,
                base_price = EXCLUDED.base_price,
                seller_id  = EXCLUDED.seller_id
        """
        rows = [
            (p["product_id"], p["name"], p["category"], p["base_price"], p["seller_id"])
            for p in products
        ]
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        print(f"dim_product : {len(rows)} produits upsertés")

    @task()
    def refresh_dim_customer():
        """Upsert des 500 clients dans dwh.dim_customer."""
        api_hook = MarketplaceAPIHook()
        pg_hook  = PostgresHook(postgres_conn_id="postgres_dwh")

        customers = api_hook.get_customers()

        sql = """
            INSERT INTO dwh.dim_customer (customer_id, email, city, signup_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                email       = EXCLUDED.email,
                city        = EXCLUDED.city,
                signup_date = EXCLUDED.signup_date
        """
        rows = [
            (c["customer_id"], c["email"], c["city"], c["signup_date"])
            for c in customers
        ]
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        print(f"dim_customer : {len(rows)} clients upsertés")

    # ── Ordre d'exécution ────────────────────────────────────────────────────
    # dim_date et dim_seller sont indépendants → peuvent tourner en parallèle
    # dim_product doit attendre dim_seller (FK)
    # dim_customer est indépendant mais tourne en dernier pour simplifier

    date_task     = refresh_dim_date()
    seller_task   = refresh_dim_seller()
    product_task  = refresh_dim_product()
    customer_task = refresh_dim_customer()

    [date_task, seller_task] >> product_task >> customer_task


marketplace_dims_refresh_daily()
