"""
DAG : marketplace_orders_ingest_daily
Pipeline ELT principal — le cœur du projet.

Flux :
    extract ──→ load_raw (MinIO)     ← en parallèle
            └─→ load_staging         ← en parallèle
                      └─→ transform_to_dwh  (DELETE + INSERT idempotent)

Règle absolue : rejouer le même run avec la même date donne toujours
le même COUNT(*) dans dwh.fact_orders. Non négociable.
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import json

from hooks.marketplace_hook import MarketplaceAPIHook

BUCKET      = "data-lake"
PG_CONN_ID  = "postgres_dwh"
S3_CONN_ID  = "minio_default"


@dag(
    dag_id="marketplace_orders_ingest_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["marketplace", "pipeline", "orders"],
)
def marketplace_orders_ingest_daily():

    @task()
    def extract(ds=None) -> list[dict]:
        """Appelle GET /orders?date={{ ds }} via MarketplaceAPIHook."""
        hook   = MarketplaceAPIHook()
        orders = hook.get_orders(ds)
        print(f"Extract : {len(orders)} commandes récupérées pour {ds}")
        return orders

    @task()
    def load_raw(orders: list, ds=None):
        """
        Sauvegarde le JSON brut dans MinIO.
        Chemin : raw/orders/dt=YYYY-MM-DD/data.json
        Si le fichier existe déjà (re-run), il est écrasé (replace=True).
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        key     = f"raw/orders/dt={ds}/data.json"
        payload = json.dumps({"date": ds, "count": len(orders), "orders": orders})

        s3_hook.load_string(
            string_data=payload,
            key=key,
            bucket_name=BUCKET,
            replace=True,
        )
        print(f"Raw : uploadé → s3://{BUCKET}/{key}")

    @task()
    def load_staging(orders: list, ds=None):
        """
        Insère les commandes dans staging.orders.
        DELETE + INSERT par date pour rester idempotent côté staging aussi.
        """
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        rows = [
            (
                o["order_id"], o["dt"], o["created_at"],
                o["seller_id"], o["product_id"], o["customer_id"],
                o["quantity"], o["unit_price"], o["total_amount"],
                o["commission"], o["status"],
            )
            for o in orders
        ]

        conn = pg_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM staging.orders WHERE dt = %s", (ds,))
                cur.executemany(
                    """
                    INSERT INTO staging.orders
                        (order_id, dt, created_at, seller_id, product_id, customer_id,
                         quantity, unit_price, total_amount, commission, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )
        print(f"Staging : {len(rows)} lignes chargées pour {ds}")

    @task()
    def transform_to_dwh(ds=None):
        """
        Pattern idempotent obligatoire (Kimball) :
            BEGIN
              DELETE FROM dwh.fact_orders WHERE dt = ds
              INSERT INTO dwh.fact_orders SELECT ... FROM staging.orders WHERE dt = ds
            COMMIT

        Pourquoi DELETE + INSERT plutôt que UPSERT :
        - Plus simple (pas de gestion de conflit ligne par ligne)
        - Plus rapide sur de gros volumes
        - État connu après chaque run : la partition dt est soit absente soit complète
        """
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        conn = pg_hook.get_conn()
        with conn:
            with conn.cursor() as cur:

                cur.execute(
                    "DELETE FROM dwh.fact_orders WHERE dt = %s",
                    (ds,),
                )

                cur.execute(
                    """
                    INSERT INTO dwh.fact_orders
                        (order_id, dt, seller_id, product_id, customer_id,
                         quantity, unit_price, total_amount, commission, status)
                    SELECT
                        order_id, dt, seller_id, product_id, customer_id,
                        quantity, unit_price, total_amount, commission, status
                    FROM staging.orders
                    WHERE dt = %s
                    """,
                    (ds,),
                )
                count = cur.rowcount

        print(f"DWH : {count} lignes insérées dans dwh.fact_orders pour {ds}")

    # ── Orchestration ────────────────────────────────────────────────────────
    # extract produit les orders (XCom)
    # load_raw et load_staging consomment les orders en parallèle
    # transform_to_dwh attend que load_staging soit terminé

    orders_data  = extract()
    raw_task     = load_raw(orders_data)       # noqa: F841
    staging_task = load_staging(orders_data)
    dwh_task     = transform_to_dwh()

    staging_task >> dwh_task


marketplace_orders_ingest_daily()
