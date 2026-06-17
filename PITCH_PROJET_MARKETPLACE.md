# Pitch — Projet A : MarketPlace Analytics

**Focus du projet :** orchestration ELT classique, idempotence, qualité de données, dashboards.

## Le pitch

Nous sommes Data Engineer dans une marketplace e-commerce, du type Etsy, Vinted ou Backmarket. Des dizaines de vendeurs y publient leurs produits, les clients achètent, et les commandes s'accumulent. Notre mission : construire la plateforme data qui alimente les dashboards business (top vendeurs, revenu par catégorie, taux de churn client) et qui détecte les anomalies commerciales, comme un effondrement des ventes ou des prix suspects.

C'est un projet d'ELT batch pur. Il est parfait pour les groupes qui veulent maîtriser les fondamentaux d'Airflow sans se disperser sur du streaming.

## Objectifs pédagogiques

À la fin du projet, on saura : construire un pipeline ELT idempotent de bout en bout (API vers S3, puis PostgreSQL, puis analytics) ; implémenter un Custom Hook pour l'API marketplace avec une authentification Bearer ; implémenter un Custom Operator de data quality avec des règles configurables ; concevoir des tables de faits et de dimensions simples, en suivant la modélisation dimensionnelle ; exposer les KPIs via un dashboard Metabase ou Streamlit ; et détecter des anomalies business simples, par exemple via un écart à la moyenne ou des seuils.

## Architecture cible

Le flux est le suivant. Une **API Marketplace** (une API Flask simulée) est la source. **Airflow 3.1.8** orchestre les DAGs ELT et extrait les données via un Custom Hook. Les données passent par deux niveaux de stockage : **MinIO** conserve le JSON brut dans une couche raw partitionnée par date (`raw/ partition dt=...`), et **PostgreSQL** sert de Data Warehouse organisé en trois schémas (`staging`, `dwh`, `analytics`). Enfin, le serving se fait via **Metabase ou Streamlit**, branché sur le DWH PostgreSQL.

En résumé du chaînage : l'API est extraite par Airflow via le Custom Hook, Airflow dépose le raw JSON dans MinIO puis charge et transforme vers PostgreSQL, et l'outil de dashboard lit PostgreSQL.

## Modèle de données cible

Le modèle est un schéma en étoile : quatre dimensions pointant chacune vers une table de faits centrale, `FACT_ORDERS`. Les dimensions sont `DIM_SELLER`, `DIM_CUSTOMER`, `DIM_PRODUCT` et `DIM_DATE`, et chacune est reliée à `FACT_ORDERS` par une relation « un à plusieurs » (un vendeur vend plusieurs commandes, un client achète plusieurs commandes, un produit apparaît dans plusieurs commandes, une date date plusieurs commandes).

Le détail des tables est le suivant. `DIM_SELLER` contient `seller_id` en clé primaire (chaîne), un `name`, un `country` et une `joined_date`. `DIM_CUSTOMER` contient `customer_id` en clé primaire, un `email`, une `city` et une `signup_date`. `DIM_PRODUCT` contient `product_id` en clé primaire, un `name`, une `category` et un `base_price` (décimal). `DIM_DATE` contient `dt` en clé primaire (date), un `year`, un `month` et un `day_of_week` (entiers). La table de faits `FACT_ORDERS` contient `order_id` en clé primaire, puis quatre clés étrangères (`seller_id`, `customer_id`, `product_id`, `dt`), une `quantity` (entier), un `total_amount` (décimal) et un `status` (chaîne).

## Découpage en deux jours

### Partie 1 — Pipeline ELT de base

L'enchaînement des étapes est : d'abord extraire via le Custom Hook de l'API ; puis uploader le brut dans MinIO en partition `dt=` ; puis charger dans le staging PostgreSQL ; puis lancer un contrôle qualité (DQ check) via le Custom Operator ; et enfin construire le DWH, c'est-à-dire les tables de dimensions et de faits.

Les livrables de cette première partie sont : le DAG `marketplace_orders_ingest_daily` qui réalise l'extraction, l'upload du raw et le load vers staging ; le Custom Hook `MarketplaceAPIHook` avec les méthodes `get_orders(date)`, `get_sellers()` et `get_products()` ; le Custom Operator `DataQualityOperator` avec cinq règles SQL configurables ; le DAG `marketplace_dwh_build_daily` qui construit les dimensions et les faits ; une idempotence testée, c'est-à-dire que relancer trois fois la même date doit donner le même résultat ; et le pattern DELETE + INSERT appliqué sur la partition `dt = {{ ds }}`.

### Partie 2 — Analytics et dashboard

La deuxième partie est au choix du groupe, 
 on finira peut-être par utiliser tableau, ou bien 

**Dashboard Metabase (la plus simple).** On ajoute Metabase dans le docker-compose (image `metabase/metabase:latest`). On crée quatre dashboards dans Metabase connecté au PostgreSQL DWH : le top 10 des vendeurs par revenu, l'évolution du CA par jour sur les sept derniers jours, la répartition des ventes par catégorie, et les clients actifs comparés aux clients dormants. On ajoute le DAG `marketplace_analytics_aggregate_daily` qui alimente les tables d'agrégation lues par Metabase.

**Dashboard Streamlit avec détection d'anomalies (la plus technique).** On ajoute un service Streamlit dans le docker-compose. On crée le DAG `marketplace_anomaly_detect_daily` qui calcule la moyenne mobile sur sept jours du CA par vendeur, flagge les vendeurs dont le CA chute de plus de 30 % par rapport à cette moyenne, et écrit les anomalies dans la table `analytics.anomalies`. On ajoute un branching : si des anomalies sont détectées, le DAG appelle un webhook (simulé). Enfin, le dashboard Streamlit affiche les KPIs ainsi que les anomalies détectées.

## La stack Docker Compose

Les services à faire tourner sont les suivants. Les services Airflow (image `apache/airflow:3.1.8`) exposent l'orchestrateur sur le port 8080. `postgres-airflow` (image `postgres:16`) sur le port 5432 sert de base de métadonnées. `postgres-dwh` (image `postgres:16`) sur le port 5433 est le Data Warehouse. `minio` (image `minio/minio`) sur les ports 9000 et 9001 est le stockage objet. `api-simulee` (une image Flask custom) sur le port 5000 est l'API marketplace. Pour l'option 1, `metabase` (image `metabase/metabase`) sur le port 3000 sert les dashboards. Pour l'option 2, un service `streamlit` (image Python custom) sur le port 8501 sert les dashboards.

## Les DAGs attendus

Au jour 1, deux DAGs : `marketplace_orders_ingest_daily` planifié `@daily`, et `marketplace_dwh_build_daily` planifié via Asset. Au jour 2, deux DAGs : `marketplace_analytics_aggregate_daily` planifié via Asset, et (pour l'option 2) `marketplace_anomaly_detect_daily` planifié `@daily`.

Les dépendances entre DAGs reposent sur des Assets. Le DAG d'ingestion produit un Asset `raw_orders` qui déclenche le DAG de build du DWH. Ce DAG de build produit à son tour un Asset `dwh_orders` qui déclenche à la fois le DAG d'agrégation analytics et le DAG de détection d'anomalies.

## Fonctionnalités bonus (si le temps le permet)

Si le MVP est terminé en avance, on peut ajouter : un système d'alerte Slack ou webhook en cas d'échec d'un DAG ; des tests pytest sur le Custom Operator avec un mock de la base de données ; un backfill manuel sur une semaine de données historiques ; et une dimension `dim_category` enrichie via un mapping externe.
