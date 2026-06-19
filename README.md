# Maelys Marketplace Analytics

Pipeline ELT batch complet pour une marketplace e-commerce simulée.  
Construit from scratch en 2 jours : Airflow 3.1.8, MinIO, PostgreSQL, Metabase, Docker Compose.

---

## Contexte

Projet réalisé dans le rôle d'une Data Engineer nouvellement recrutée chez Maelys Marketplace — une marketplace française qui met en relation des vendeurs indépendants et des consommateurs. L'objectif : remplacer des rapports Excel manuels par un pipeline de données automatisé, idempotent, et exposé dans des dashboards interactifs.

Aucune infrastructure data n'existait. Tout a été construit from scratch.

---

## Architecture

```mermaid
flowchart LR
    subgraph Source
        API[API Flask\napi-marketplace:5000]
    end

    subgraph Orchestration
        AF[Airflow 3.1.8\nCeleryExecutor]
        HOOK[MarketplaceAPIHook]
    end

    subgraph Storage
        MINIO[(MinIO\nData Lake\nraw JSON)]
        STG[(PostgreSQL\nstaging.orders)]
        DWH[(PostgreSQL\ndwh.fact_orders\n+ 4 dimensions)]
        ANA[(PostgreSQL\nanalytics.*)]
    end

    subgraph Serving
        MB[Metabase\nDashboards]
    end

    API -->|GET /orders?date=ds| HOOK
    HOOK --> AF
    AF -->|PUT raw/orders/dt=.../data.json| MINIO
    AF -->|INSERT idempotent| STG
    STG -->|DELETE + INSERT par dt| DWH
    DWH -->|agrégations| ANA
    ANA --> MB
```

### Flux d'un run quotidien

1. Le scheduler Airflow déclenche les DAGs pour la date `{{ ds }}`
2. `MarketplaceAPIHook` appelle `GET /orders?date=ds` avec auth Bearer
3. Le JSON brut est archivé dans MinIO (`raw/orders/dt=YYYY-MM-DD/data.json`)
4. Les données sont chargées dans `staging.orders`
5. Transformation idempotente vers `dwh.fact_orders` (DELETE + INSERT par partition)
6. Les tables analytics sont agrégées
7. Metabase lit les agrégations pour les dashboards

---

## Stack technique

| Service | Image | Port | Rôle |
|---|---|---|---|
| airflow-apiserver | apache/airflow:3.1.8 | 8080 | UI + API REST |
| airflow-scheduler | apache/airflow:3.1.8 | — | Déclenchement des DAGs |
| airflow-worker | apache/airflow:3.1.8 | — | Exécution des tâches |
| airflow-dag-processor | apache/airflow:3.1.8 | — | Parsing des DAGs |
| airflow-triggerer | apache/airflow:3.1.8 | — | Opérations deferrable |
| postgres-airflow | postgres:16 | 5432 | Métadonnées Airflow |
| postgres-dwh | postgres:16 | 5433 | Data Warehouse |
| redis | redis:7 | 6379 | Broker Celery |
| minio | minio/minio | 9000/9001 | Data Lake (stockage objet S3) |
| api-marketplace | Flask (custom) | 5000 | API source simulée |
| metabase | metabase:v0.59.6.1 | 3000 | Dashboards BI |

---

## Lancer le projet

### Prérequis

- Docker Desktop
- Git

### Démarrage

```bash
git clone <repo>
cd Marketplace

# Copier les variables d'environnement
cp .env.example .env

# Démarrer tous les services (~60s)
docker compose up -d

# Vérifier que tout est healthy
docker compose ps
```

### Accès

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minio_admin / minio_password_2026 |
| Metabase | http://localhost:3000 | admin@maelys.local / Admin2026! |
| API Marketplace | http://localhost:5000/health | — |

### Tester l'API

```bash
curl -H "Authorization: Bearer formation-token-2026" \
     "http://localhost:5000/orders?date=2026-04-07"
```

### Générer les données du mois d'avril

```bash
# Depuis l'UI Airflow : déclencher un backfill sur marketplace_orders_ingest_daily
# ou via la CLI pour une date précise :
docker compose exec airflow-worker \
  airflow dags test marketplace_orders_ingest_daily 2026-04-07
```

---

## DAGs

### `marketplace_dims_refresh_daily`

Rafraîchit les dimensions `dim_seller`, `dim_product`, `dim_customer`.  
Stratégie : INSERT ... ON CONFLICT DO UPDATE (upsert). Si un vendeur change de nom, la mise à jour est propagée.

```
refresh_sellers → refresh_products → refresh_customers
```

### `marketplace_orders_ingest_daily`

Pipeline principal. Extract → raw → staging → DWH.

```
extract_orders
    → upload_to_minio        (archive JSON brut)
    → load_to_staging        (staging.orders)
    → transform_to_dwh       (DELETE + INSERT par dt — idempotent)
```

L'idempotence est garantie : rejouer le même run 10 fois donne toujours le même `COUNT(*)` dans `dwh.fact_orders`.

### `marketplace_analytics_aggregate_daily`

Construit les tables analytics pour Metabase.

```
aggregate_daily_summary  ┐
aggregate_seller_daily   ├─→ aggregate_customer_activity
aggregate_category_daily ┘
```

`seller_daily` utilise DELETE + INSERT (pas UPSERT) pour que les vendeurs inactifs disparaissent proprement des agrégations journalières.

### `marketplace_anomaly_detect_daily`

Détecte les chutes de CA par rapport à la moyenne mobile 7 jours.

**Règle** : si CA du jour < 70% de la moyenne des 7 jours précédents → anomalie insérée dans `analytics.anomalies`.

| Sévérité | Condition |
|---|---|
| warning | -30% à -50% |
| critical | < -50% |

**Résultats sur avril 2026 :**

| Date | CA réel | Moyenne 7j | Écart | Sévérité |
|---|---|---|---|---|
| 2026-04-15 | 58 021 € | 150 342 € | -61.41% | CRITICAL |
| 2026-04-25 | 56 517 € | 171 208 € | -66.99% | CRITICAL |

---

## Modèle de données

Schéma en étoile (méthode Kimball), 3 couches séparées dans PostgreSQL.

```
staging.orders          ← données brutes typées, chargées depuis l'API

dwh.dim_seller          ← vendeurs (seller_id, name, country, joined_date)
dwh.dim_product         ← produits (product_id, name, category, price)
dwh.dim_customer        ← clients (customer_id, country, joined_date)
dwh.dim_date            ← calendrier (dt, year, month, quarter, day_of_week)
dwh.fact_orders         ← table de faits (order_id, dt, seller_id, product_id,
                           customer_id, quantity, total_amount, commission, status)

analytics.daily_summary      ← CA par jour
analytics.seller_daily       ← CA par vendeur par jour
analytics.category_daily     ← CA par catégorie par jour
analytics.customer_activity  ← activité totale par client (full recompute)
analytics.anomalies          ← anomalies détectées
```

---

## Dashboards Metabase

Deux dashboards avec filtres de période dynamiques (field filters sur `dt`).

### Dashboard 1 — Executive Summary

- CA total sur la période (Big Number)
- Nombre de commandes sur la période (Big Number)
- Panier moyen (Big Number)
- Évolution du CA jour par jour (line chart)
- Top 5 vendeurs du dernier jour (bar chart)

### Dashboard 2 — Top Sellers

- Top 10 vendeurs sur la période, avec leurs noms (bar chart)
- Évolution du CA des 3 meilleurs vendeurs, ventilée par vendeur (line chart)
- Vendeurs inactifs depuis plus de 7 jours (table — sans filtre de date, mesure toujours sur les données les plus récentes)

---

## Tests

```bash
docker compose exec airflow-worker pytest tests/ -v
```

```
tests/test_dags.py::test_no_import_errors                          PASSED
tests/test_dags.py::test_all_dags_catchup_false                    PASSED
tests/test_dags.py::test_marketplace_ingest_has_idempotent_transform PASSED
```

Les 3 tests couvrent :
1. Absence d'erreurs d'import sur tous les DAGs
2. `catchup=False` sur tous les DAGs (pas de backfill automatique non voulu)
3. Présence de la tâche `transform_to_dwh` dans le DAG d'ingestion (idempotence)

---

## Custom Hook

`plugins/hooks/marketplace_hook.py` — hérite de `BaseHook`, lit la Connection Airflow `marketplace_api` (host + token Bearer), expose :

- `get_orders(date: str) → list[dict]`
- `get_sellers() → list[dict]`
- `get_products() → list[dict]`
- `get_customers() → list[dict]`

L'auth Bearer est lue depuis `Connection.password` — jamais hardcodée dans le code.

---

## Choix techniques

**MinIO comme couche raw** : archiver le JSON brut permet de rejouer le pipeline depuis l'archive si la transformation a un bug, sans rappeler l'API source. Pattern "data lake first".

**DELETE + INSERT plutôt qu'UPSERT** : plus simple à raisonner (la partition est dans un état connu après chaque run), plus performant sur des lots > 1 000 lignes, et ça gère proprement la disparition de vendeurs inactifs.

**Trois schémas PostgreSQL séparés** : `staging` (typage brut), `dwh` (modèle dimensionnel propre), `analytics` (agrégations pré-calculées). Plus maintenable qu'un schéma fourre-tout.

**Metabase field filters** : les dashboards utilisent `WHERE {{date}}` (field filters Metabase) plutôt que des dates hardcodées, ce qui permet une sélection de période interactive sans réécrire les requêtes.

---

## Structure du projet

```
Marketplace/
├── api/                         # API Flask simulée
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   ├── marketplace_dims_refresh_daily.py
│   ├── marketplace_orders_ingest_daily.py
│   ├── marketplace_analytics_aggregate_daily.py
│   └── marketplace_anomaly_detect_daily.py
├── plugins/hooks/
│   └── marketplace_hook.py
├── init-db/
│   └── schema.sql               # CREATE SCHEMA + CREATE TABLE
├── tests/
│   └── test_dags.py
├── airflow/
│   └── passwords.json
├── docker-compose.yaml
├── .env.example
└── README.md
```

---

## Commandes utiles

```bash
# Vérifier l'état des services
docker compose ps

# Logs d'un service en temps réel
docker compose logs -f airflow-scheduler --tail=50

# Tester un DAG manuellement
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07

# Vérifier l'idempotence
docker compose exec postgres-dwh psql -U dwh_user -d dwh \
  -c "SELECT COUNT(*) FROM dwh.fact_orders WHERE dt='2026-04-07';"
# Relancer le dag test, recompter → même résultat

# Tester la détection d'anomalies
docker compose exec airflow-worker airflow dags test marketplace_anomaly_detect_daily 2026-04-15

# Voir les anomalies en base
docker compose exec postgres-dwh psql -U dwh_user -d dwh \
  -c "SELECT * FROM analytics.anomalies ORDER BY dt;"

# Arrêter sans perdre les données
docker compose down

# Reset complet (supprime les volumes)
docker compose down -v
```
