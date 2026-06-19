"""
Tests pytest du pipeline Maelys Marketplace.

Lancez depuis le conteneur airflow-worker :
    docker compose exec airflow-worker pytest tests/ -v

Les 3 tests couvrent :
    1. Absence d'erreurs d'import des DAGs
    2. catchup=False sur tous les DAGs (pas de backfill automatique non voulu)
    3. Présence de la tâche transform_to_dwh dans le DAG d'ingestion (idempotence)
"""

import pytest
from airflow.models import DagBag

DAG_FOLDER = "/opt/airflow/dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    """Aucun DAG ne doit lever d'erreur à l'import."""
    assert dagbag.import_errors == {}, (
        f"Erreurs d'import détectées : {dagbag.import_errors}"
    )


def test_all_dags_catchup_false(dagbag):
    """Tous les DAGs doivent avoir catchup=False pour éviter les backfills non voulus."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, (
            f"Le DAG '{dag_id}' a catchup=True — risque de backfill non voulu"
        )


def test_marketplace_ingest_has_idempotent_transform(dagbag):
    """Le DAG d'ingestion doit contenir la tâche transform_to_dwh (DELETE + INSERT)."""
    dag = dagbag.get_dag("marketplace_orders_ingest_daily")
    assert dag is not None, "DAG 'marketplace_orders_ingest_daily' introuvable"

    task_ids = [task.task_id for task in dag.tasks]
    assert "transform_to_dwh" in task_ids, (
        f"Tâche 'transform_to_dwh' absente. Tâches trouvées : {task_ids}"
    )
