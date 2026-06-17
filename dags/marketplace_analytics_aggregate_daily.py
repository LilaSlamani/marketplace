"""
DAG : marketplace_analytics_aggregate_daily
Construit les tables d'agrégation pour Metabase.

Lit dwh.fact_orders et remplit :
    - analytics.daily_summary      → CA par jour
    - analytics.seller_daily       → CA par vendeur par jour
    - analytics.category_daily     → CA par catégorie par jour
    - analytics.customer_activity  → activité totale par client (full recompute)

Stratégie : INSERT ... ON CONFLICT DO UPDATE (upsert)
→ idempotent : rejouer le même run donne le même résultat.
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

PG_CONN_ID = "postgres_dwh"


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["marketplace", "analytics"],
)
def marketplace_analytics_aggregate_daily():

    @task()
    def aggregate_daily_summary(ds=None):
        """
        CA global par jour.
        total_revenue et total_commission = completed uniquement.
        total_orders = toutes commandes (pour le taux d'annulation).
        """
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        sql = """
            INSERT INTO analytics.daily_summary
                (dt, total_orders, completed_orders, total_revenue, total_commission)
            SELECT
                dt,
                COUNT(*)                                              AS total_orders,
                COUNT(*) FILTER (WHERE status = 'completed')          AS completed_orders,
                COALESCE(SUM(total_amount)  FILTER (WHERE status = 'completed'), 0) AS total_revenue,
                COALESCE(SUM(commission)    FILTER (WHERE status = 'completed'), 0) AS total_commission
            FROM dwh.fact_orders
            WHERE dt = %(ds)s
            GROUP BY dt
            ON CONFLICT (dt) DO UPDATE SET
                total_orders      = EXCLUDED.total_orders,
                completed_orders  = EXCLUDED.completed_orders,
                total_revenue     = EXCLUDED.total_revenue,
                total_commission  = EXCLUDED.total_commission
        """
        pg_hook.run(sql, parameters={"ds": ds})
        print(f"daily_summary : agrégé pour {ds}")

    @task()
    def aggregate_seller_daily(ds=None):
        """CA par vendeur par jour."""
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        sql = """
            INSERT INTO analytics.seller_daily
                (dt, seller_id, total_orders, completed_orders, total_revenue)
            SELECT
                dt,
                seller_id,
                COUNT(*)                                              AS total_orders,
                COUNT(*) FILTER (WHERE status = 'completed')          AS completed_orders,
                COALESCE(SUM(total_amount) FILTER (WHERE status = 'completed'), 0) AS total_revenue
            FROM dwh.fact_orders
            WHERE dt = %(ds)s
            GROUP BY dt, seller_id
            ON CONFLICT (dt, seller_id) DO UPDATE SET
                total_orders     = EXCLUDED.total_orders,
                completed_orders = EXCLUDED.completed_orders,
                total_revenue    = EXCLUDED.total_revenue
        """
        pg_hook.run(sql, parameters={"ds": ds})
        print(f"seller_daily : agrégé pour {ds}")

    @task()
    def aggregate_category_daily(ds=None):
        """
        CA par catégorie par jour.
        Jointure fact_orders → dim_product pour récupérer la catégorie.
        """
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        sql = """
            INSERT INTO analytics.category_daily
                (dt, category, total_orders, completed_orders, total_revenue)
            SELECT
                fo.dt,
                dp.category,
                COUNT(*)                                               AS total_orders,
                COUNT(*) FILTER (WHERE fo.status = 'completed')        AS completed_orders,
                COALESCE(SUM(fo.total_amount) FILTER (WHERE fo.status = 'completed'), 0) AS total_revenue
            FROM dwh.fact_orders fo
            JOIN dwh.dim_product dp ON fo.product_id = dp.product_id
            WHERE fo.dt = %(ds)s
            GROUP BY fo.dt, dp.category
            ON CONFLICT (dt, category) DO UPDATE SET
                total_orders     = EXCLUDED.total_orders,
                completed_orders = EXCLUDED.completed_orders,
                total_revenue    = EXCLUDED.total_revenue
        """
        pg_hook.run(sql, parameters={"ds": ds})
        print(f"category_daily : agrégé pour {ds}")

    @task()
    def aggregate_customer_activity():
        """
        Activité totale par client — full recompute sur tout l'historique.
        Pas de filtre sur ds : on recalcule sur toutes les dates connues.
        Nécessaire pour que last_order_date soit toujours à jour.
        """
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        sql = """
            INSERT INTO analytics.customer_activity
                (customer_id, last_order_date, total_orders, total_revenue)
            SELECT
                customer_id,
                MAX(dt)             AS last_order_date,
                COUNT(*)            AS total_orders,
                SUM(total_amount)   AS total_revenue
            FROM dwh.fact_orders
            WHERE status = 'completed'
            GROUP BY customer_id
            ON CONFLICT (customer_id) DO UPDATE SET
                last_order_date = EXCLUDED.last_order_date,
                total_orders    = EXCLUDED.total_orders,
                total_revenue   = EXCLUDED.total_revenue
        """
        pg_hook.run(sql)
        print("customer_activity : recompute complet terminé")

    # ── Orchestration ────────────────────────────────────────────────────────
    # Les 3 agrégations par date tournent en parallèle
    # customer_activity tourne après (full recompute sur tout l'historique)

    summary  = aggregate_daily_summary()
    sellers  = aggregate_seller_daily()
    cats     = aggregate_category_daily()
    activity = aggregate_customer_activity()

    [summary, sellers, cats] >> activity


marketplace_analytics_aggregate_daily()
