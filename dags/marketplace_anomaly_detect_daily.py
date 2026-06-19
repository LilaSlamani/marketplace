"""
DAG : marketplace_anomaly_detect_daily
Détecte les anomalies commerciales basées sur la moyenne mobile 7 jours.

Règle métier : si CA du jour < 70 % de la moyenne des 7 jours précédents
→ anomalie détectée → insertion dans analytics.anomalies

Sévérité :
    warning  : CA entre -30 % et -50 % de la moyenne
    critical : CA en dessous de -50 % de la moyenne
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

PG_CONN_ID = "postgres_dwh"


@dag(
    dag_id="marketplace_anomaly_detect_daily",
    schedule="@daily",
    start_date=datetime(2026, 4, 8),
    catchup=False,
    tags=["marketplace", "anomalies"],
)
def marketplace_anomaly_detect_daily():

    @task()
    def detect_anomalies(ds=None):
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        # Calcule la moyenne mobile sur les 7 jours PRÉCÉDENTS (pas le jour courant)
        # et compare avec le CA du jour
        sql_check = """
            WITH moving_avg AS (
                SELECT
                    dt,
                    total_revenue,
                    AVG(total_revenue) OVER (
                        ORDER BY dt
                        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                    ) AS avg_7d
                FROM analytics.daily_summary
                WHERE dt <= %(ds)s
            )
            SELECT
                dt,
                total_revenue                                      AS actual,
                ROUND(avg_7d, 2)                                   AS expected,
                ROUND((total_revenue / avg_7d - 1) * 100, 2)      AS deviation_pct
            FROM moving_avg
            WHERE dt = %(ds)s
              AND avg_7d IS NOT NULL
              AND total_revenue < avg_7d * 0.70
        """

        conn = pg_hook.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_check, {"ds": ds})
                rows = cur.fetchall()

                for dt, actual, expected, deviation_pct in rows:
                    severity = "critical" if deviation_pct < -50 else "warning"

                    cur.execute(
                        """
                        INSERT INTO analytics.anomalies
                            (dt, detected_at, scope, metric, expected, actual, deviation_pct, severity)
                        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s)
                        """,
                        (dt, "global", "total_revenue", expected, actual, deviation_pct, severity),
                    )

                    print(
                        f"Anomalie {severity.upper()} détectée le {dt} : "
                        f"CA réel={actual:.2f}€, moyenne_7j={expected:.2f}€, "
                        f"écart={deviation_pct:.2f}%"
                    )

                if not rows:
                    print(f"Aucune anomalie détectée pour {ds}")
                else:
                    print(f"{len(rows)} anomalie(s) insérée(s) pour {ds}")

    detect_anomalies()


marketplace_anomaly_detect_daily()
