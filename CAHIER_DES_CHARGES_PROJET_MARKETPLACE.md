# Cahier des charges — Projet A : MarketPlace Analytics

**Version :** 3.0 (avril 2026)

## 1. Contexte

Nous sommes Data Engineer dans **Maelys Marketplace**, une marketplace e-commerce française qui met en relation des vendeurs indépendants (artisans, marques DNVB, revendeurs) et des consommateurs finaux. Maelys prélève une commission de 12 % sur chaque vente.

Pour les ordres de grandeur (chiffres fictifs) : environ 2 400 vendeurs actifs, 180 000 produits, à peu près 8 500 commandes par jour, et un chiffre d'affaires mensuel d'environ 4,2 M€. L'entreprise compte 75 personnes en interne, dont un seul analyste qui passe actuellement deux jours par semaine à extraire les données du backoffice à la main pour produire des rapports Excel.

Le besoin. Le CEO veut un dashboard hebdomadaire avec les KPIs clés (top vendeurs, CA par catégorie, panier moyen). L'équipe finance a besoin de données fiables pour la clôture mensuelle et la facturation des commissions. Le responsable catalogue veut détecter les anomalies (effondrement des ventes, prix suspects). Aujourd'hui tout est manuel : les rapports sont en retard, les erreurs de copier-coller sont fréquentes, et l'analyste passe 40 % de son temps sur de l'extraction au lieu de faire de l'analyse.

La mission. Construire en deux jours un MVP de plateforme data qui ingère automatiquement les données depuis l'API du backoffice, les stocke dans un Data Warehouse propre suivant une modélisation dimensionnelle, expose des KPIs lisibles via un outil de BI (Metabase), et détecte les anomalies commerciales basiques. Nous sommes l'équipe Data nouvellement recrutée et aucune infrastructure data n'existe : on part de zéro. Le code doit être maintenable, testé et reproductible via Docker Compose.

## 2. Objectifs pédagogiques

À la fin du projet, on saura : construire un pipeline ELT idempotent de bout en bout avec Airflow 3.1.8 ; implémenter un Custom Hook pour une API REST authentifiée ; modéliser un schéma dimensionnel en étoile (méthode Kimball) ; utiliser MinIO comme couche raw (un data lake compatible S3) ; exposer des KPIs business via Metabase ; brancher Prometheus et Grafana pour la supervision technique du pipeline (en bonus) ; et tester un pipeline avec pytest.

## 3. Architecture cible

### 3.1 Vue d'ensemble

Le flux complet est le suivant. La source est l'API Marketplace (une API Flask simulée). Côté orchestration, le `MarketplaceAPIHook` alimente Airflow 3.1.8, qui exécute quatre à cinq DAGs. Côté stockage, Airflow uploade le raw JSON dans MinIO (partition `raw partition dt=`) et fait le load puis le transform dans PostgreSQL 18, organisé en trois schémas (`staging`, `dwh`, `analytics`). Côté serving BI, Metabase (avec ses quatre dashboards) lit PostgreSQL. En bonus, une couche d'observabilité collecte les métriques : Airflow envoie ses métriques StatsD à Prometheus, un postgres-exporter envoie les métriques PostgreSQL à Prometheus, et Grafana lit Prometheus.

### 3.2 Flux d'un pipeline run

Étape par étape : le scheduler déclenche l'extraction pour une date donnée (`{{ ds }}`) ; le `MarketplaceAPIHook` fait un GET sur `/orders?date=...` ; l'API répond avec le JSON de N commandes ; le Hook fait un PUT du fichier `raw/orders/dt=.../data.json` dans MinIO ; le Hook fait un INSERT idempotent dans `staging.orders` ; le scheduler fait un DELETE puis un INSERT dans `dwh.fact_orders` pour la date concernée ; le scheduler rafraîchit `analytics.daily_summary` ; et Metabase fait ses SELECT pour le dashboard.

### 3.3 Pourquoi cette architecture

**MinIO comme couche raw :** conserver le JSON brut en archive permet de rejouer un pipeline si la transformation a un bug, sans devoir rappeler l'API source. C'est le pattern « data lake first ».

**Trois schémas PostgreSQL séparés :** `staging` (données brutes typées), `dwh` (modèle dimensionnel propre) et `analytics` (agrégations pré-calculées pour les dashboards). C'est plus propre qu'un seul schéma fourre-tout.

**Metabase plutôt que Superset :** plus simple à mettre en place, suffisant pour quatre dashboards, et l'installation Docker est triviale.

**Pas de Spark ni dbt :** Airflow gère lui-même les transformations SQL via `PostgresHook.run()`. Pour 8 500 commandes par jour, c'est largement suffisant.

## 4. Stack technique

Les versions et tags Docker sont précisés dans la documentation commune.

### 4.1 Services Docker Compose

Côté Airflow, cinq services : `airflow-apiserver` sur le port 8080 (l'UI et l'API REST) ; `airflow-scheduler` (le scheduler) ; `airflow-dag-processor` (le parsing des DAGs) ; `airflow-triggerer` (les opérations deferrable) ; et `airflow-worker` (l'exécution des tâches).

Côté bases et stockage : `postgres-airflow` sur le port 5432, volume `pg-airflow-data`, est la base de métadonnées ; `postgres-dwh` sur le port 5433, volume `pg-dwh-data`, est le Data Warehouse ; `redis` sur le port 6379 est le broker Celery ; `minio` sur les ports 9000 et 9001, volume `minio-data`, est le stockage objet ; et `minio-init` crée le bucket `data-lake` au démarrage.

Côté sources et serving : `api-marketplace` sur le port 5000 est l'API Flask simulée ; et `metabase` sur le port 3000, volume `metabase-data`, est l'outil de BI à ajouter au jour 2.

Les services bonus : `prometheus` sur le port 9090, volume `prometheus-data`, pour la supervision ; `grafana` sur le port 3001, volume `grafana-data`, pour les dashboards techniques ; `postgres-exporter` sur le port 9187 pour les métriques PostgreSQL ; et `statsd-exporter` sur le port 9102 pour les métriques Airflow.

### 4.2 Comment lire le docker-compose.yaml

Le fichier fourni dans le kit a une structure standard. Il définit un bloc partagé `x-airflow-common` (une ancre YAML `&airflow-common`) qui contient l'image `apache/airflow:3.1.8`, un bloc de variables d'environnement communes (`&airflow-common-env`, avec par exemple `AIRFLOW__CORE__EXECUTOR` à `CeleryExecutor`), et une liste de volumes : `./dags:/opt/airflow/dags` où sont montés nos DAGs, `./plugins:/opt/airflow/plugins` où vont nos hooks et operators custom, et `./logs:/opt/airflow/logs`. Chaque service Airflow hérite de ce bloc commun via `<<: *airflow-common` et ajoute sa propre commande (par exemple `command: scheduler`).

Point d'attention : si on modifie le bloc `x-airflow-common`, tous les services Airflow doivent être redémarrés ensemble avec `docker compose up -d --force-recreate`.

### 4.3 Commandes Docker indispensables

Pour le démarrage initial (qui crée les volumes et initialise la base) : `docker compose up -d`, puis attendre une soixantaine de secondes que tous les services soient healthy. Pour vérifier l'état des services : `docker compose ps`. Pour suivre les logs d'un service en temps réel : `docker compose logs -f airflow-scheduler --tail=50`. Pour entrer dans un conteneur en debug : `docker compose exec airflow-worker bash`. Pour tester un DAG en mode test sans passer par le scheduler : `docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07`. Pour recréer un service après modification du compose : `docker compose up -d --force-recreate metabase`. Pour tout arrêter en conservant les volumes : `docker compose down`. Pour un reset complet qui supprime les volumes (attention aux données) : `docker compose down -v`.

## 5. API Marketplace (fournie)

L'API Flask simulée tourne dans le conteneur `api-marketplace` et expose ses endpoints avec une authentification Bearer dont le token est `formation-token-2026`.

Les endpoints. `GET /health` (sans paramètre) renvoie le statut et la version, et ne demande pas d'authentification. `GET /orders?date=YYYY-MM-DD` renvoie une liste de commandes et demande l'authentification. `GET /sellers?limit=...` renvoie une liste de vendeurs (auth requise). `GET /products` renvoie le catalogue (auth requise). `GET /customers?limit=...` renvoie une liste de clients (auth requise).

### 5.1 Pourquoi cette API est « déterministe »

L'API génère ses données à partir d'un hash de la date (`seed = md5(date)`). Conséquence : une même date donne toujours le même résultat. C'est ce qui permet de tester l'idempotence — rejouer un run et vérifier que les données sont identiques.

### 5.2 Tester l'API à la main

Pour le health check : `curl http://localhost:5000/health`. Avec authentification : `curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/orders?date=2026-04-07"`. Si on oublie le header `Authorization`, l'API renvoie volontairement un `401 Unauthorized` — c'est fait exprès pour nous forcer à utiliser une Connection Airflow plutôt que de hardcoder le token.

## 6. User stories

Les sept besoins exprimés. Le CEO veut voir le CA total par jour sur 30 jours (priorité Must). Le CEO veut connaître le top 10 des vendeurs par revenu (Must). La finance veut un récapitulatif des commissions par jour (Must). Le responsable catalogue veut voir la répartition du CA par catégorie (priorité Should). L'analyste veut être alerté si le CA du jour passe sous 70 % de la moyenne sur 7 jours (Should). L'analyste veut pouvoir rejouer le pipeline sur une date passée (priorité Could). Les Ops veulent superviser l'état du pipeline et être alertés si un DAG échoue (Could).

## 7. Périmètre — MVP vs Bonus

### 7.1 Must have (MVP, obligatoire)

Un Custom Hook `MarketplaceAPIHook` avec des méthodes pour `/orders`, `/sellers` et `/products`. Le pipeline ELT principal, qui extrait via le Hook, uploade le raw JSON dans MinIO (partition `dt=YYYY-MM-DD`), charge dans `staging.orders`, puis transforme vers `dwh.fact_orders` de façon idempotente (DELETE puis INSERT par partition). Un modèle dimensionnel simple avec au moins `dim_seller`, `dim_product` et `fact_orders`. Deux dashboards Metabase : CA journalier et top vendeurs. Et au moins trois tests pytest, dont un qui vérifie l'idempotence.

### 7.2 Should have (bonus, environ +5 points)

Un Custom Operator `DataQualityOperator` avec trois règles configurables (not null, not empty, pas de date dans le futur). Un branching dans le DAG : si le contrôle qualité échoue, une tâche d'alerte est déclenchée au lieu du load vers le DWH. Et deux dashboards Metabase supplémentaires (commissions, catégories).

### 7.3 Could have (bonus, environ +10 points)

La stack d'observabilité Prometheus et Grafana. Un DAG de détection d'anomalies basé sur l'écart à la moyenne mobile 7 jours. Un backfill manuel documenté sur 7 jours d'historique. Et des tests pytest avec mock de la base de données.

### 7.4 Won't have (à ne pas chercher à faire)

Des dimensions de type SCD2 (du sur-engineering pour ce projet). Du CI/CD GitHub Actions. Une migration vers le cloud.

Règle d'or : livrer le MVP à 100 % avant toute tentative de bonus. Un MVP propre et testé vaut mieux qu'un projet ambitieux bancal.

## 8. Modèle de données (à concevoir)

### 8.1 Contraintes

On doit concevoir soi-même le schéma en étoile (Kimball). Les contraintes : les schémas `staging`, `dwh` et `analytics` doivent être séparés ; il faut au moins trois dimensions et une table de faits ; les dimensions doivent avoir une PRIMARY KEY ; la table de faits doit avoir des FOREIGN KEY vers les dimensions ; et il faut au moins une table d'agrégation dans `analytics`.

### 8.2 Pourquoi un schéma en étoile

Pour les performances (les jointures dimension–fait sont optimisées par PostgreSQL), la lisibilité (un analyste comprend immédiatement la structure), le reporting (Metabase est conçu pour ce modèle et détecte automatiquement les relations) et l'évolutivité (ajouter une dimension ne casse rien).

### 8.3 Conseil de design

Commencer simple. La dimension `dim_seller` : `id`, `name`, `country`, `joined_date`. La dimension `dim_product` : `id`, `name`, `category`, `seller_id`. La dimension `dim_date` : `dt`, `year`, `month`, `day_of_week`. La table de faits `fact_orders` : `order_id`, `dt`, `seller_id`, `product_id`, `quantity`, `total`, `commission`. Et la table d'agrégation `analytics.daily_summary` : `dt`, `total_orders`, `total_revenue`, `top_seller_id`.

Conseil : ne pas sur-modéliser, quatre tables propres valent mieux que douze tables bancales. Le livrable est un fichier `init-db/schema.sql` contenant les `CREATE SCHEMA` et `CREATE TABLE`.

## 9. Spécifications techniques

### 9.1 DAGs attendus (noms suggérés)

`marketplace_dims_refresh_daily`, planifié `@daily`, rafraîchit `dim_seller` et `dim_product`. `marketplace_orders_ingest_daily`, planifié `@daily`, est le pipeline principal (extract, raw, staging, dwh). `marketplace_analytics_aggregate_daily`, planifié via Asset, construit les tables analytics. Et en bonus, `marketplace_anomaly_detect_daily`, planifié `@daily`, fait la détection d'anomalies sur le CA.

### 9.2 Custom Hook (à designer)

Le `MarketplaceAPIHook` doit hériter de `BaseHook`, lire la Connection Airflow `marketplace_api` (où le host est l'URL et le password est le token Bearer), exposer au minimum `get_orders(date)`, `get_sellers()` et `get_products()`, gérer les erreurs HTTP (timeout, 5xx) avec un retry, et être testable unitairement en mockant `requests`. On conçoit soi-même la signature et l'implémentation : ne pas chercher un template tout prêt, c'est l'exercice.

### 9.3 Pattern d'idempotence (obligatoire)

Pour chaque run daté `{{ ds }}`, la transformation de `staging` vers `dwh` doit faire, dans une transaction : `BEGIN`, puis `DELETE FROM dwh.fact_orders WHERE dt = '{{ ds }}'`, puis `INSERT INTO dwh.fact_orders SELECT ... FROM staging.orders WHERE dt = '{{ ds }}'`, puis `COMMIT`.

On utilise DELETE + INSERT plutôt qu'un MERGE ou un UPSERT parce que c'est plus simple (pas de gestion de conflit ligne par ligne), plus performant pour des lots de plus de 1 000 lignes, et plus facile à débugger (la table est dans un état connu après chaque run). Règle absolue : rejouer un run avec la même date donne le même résultat en base. Non négociable.

## 10. Metabase

### 10.1 Configuration Docker

Service à ajouter dans le docker-compose : `metabase`, image `metabase/metabase:v0.59.6.1`, port 3000 mappé sur 3000, variables d'environnement `MB_DB_TYPE` à `h2` et `MB_DB_FILE` à `/metabase-data/metabase.db`, volume `metabase-data` monté sur `/metabase-data`, dépendance sur `postgres-dwh`, et rattaché au réseau `airflow-net`.

Le volume `metabase-data` est crucial : sans lui, les dashboards seront perdus à chaque `docker compose down -v`. Pour une persistance encore meilleure, on peut utiliser PostgreSQL au lieu de H2 comme base de métadonnées de Metabase (plus complexe mais production-grade).

### 10.2 Premier login

Aller sur `http://localhost:3000`, créer un compte admin (en formation : `admin@maelys.local` / `Admin2026!`), puis ajouter une source de données de type PostgreSQL. Attention : le host est `postgres-dwh` (le nom Docker, pas `localhost`), le port est `5432` (le port interne, pas le 5433), la base est `dwh`, et le username/password sont ceux du fichier `.env`.

### 10.3 Dashboards (MVP : deux minimum)

Le premier dashboard, « Executive Summary », contient un grand chiffre du CA total du jour (Big Number), un graphe en ligne du CA des 30 derniers jours, et un graphe en barres du top 5 des vendeurs du jour. Le second, « Top Sellers », contient un graphe en barres du top 10 des vendeurs du mois, un graphe en ligne de l'évolution du CA des trois meilleurs vendeurs sur 30 jours, et une table des vendeurs inactifs (plus de 7 jours sans vente).

Conseil : les questions Metabase peuvent être créées en mode visuel (sans SQL) ou via l'éditeur SQL natif. Pour les KPIs simples, le mode visuel suffit ; pour les agrégations complexes, écrire du SQL est plus rapide.

## 11. Découpage du projet

### 11.1 Construire le pipeline ELT

L'objectif de la première partie est d'avoir un pipeline qui tourne de bout en bout, même minimaliste. À la fin de la journée, on doit pouvoir déclencher un DAG dans l'UI Airflow et constater que des données arrivent dans `dwh.fact_orders`.

Les étapes recommandées, dans l'ordre. Démarrer le stack (`docker compose up -d`) et vérifier que tous les services sont healthy ; c'est le bon moment pour explorer l'UI Airflow et les services PostgreSQL et MinIO. Tester l'API à la main avec `curl` pour comprendre son format de retour, ce qui est plus rapide que de débugger un DAG qui appelle une API qu'on ne connaît pas. Concevoir le modèle dimensionnel sur papier ou en Mermaid avant d'écrire du SQL : identifier les dimensions et les faits, puis écrire `init-db/schema.sql` et le lancer manuellement avec `psql` pour vérifier qu'il passe. Implémenter le `MarketplaceAPIHook`, qui est la pierre angulaire de tout le pipeline : encapsuler l'auth Bearer (lue depuis la Connection) et exposer `get_orders`, `get_sellers`, `get_products` ; le tester isolément via un script Python avant de l'intégrer dans un DAG. Écrire le DAG d'ingestion étape par étape : commencer par une seule task qui extrait et imprime le résultat, puis ajouter l'upload MinIO (via `S3Hook`), puis le load dans `staging.orders` (via `PostgresHook`), sans sauter d'étape et en validant chaque task individuellement. Implémenter la transformation `staging` vers `dwh` avec le pattern idempotent obligatoire (DELETE puis INSERT par partition `dt = {{ ds }}`). Tester l'idempotence en rejouant trois fois le même run et en vérifiant que `COUNT(*)` reste identique avec `psql`.

Objectif : un pipeline principal qui tourne, l'idempotence prouvée, des données visibles dans `dwh.fact_orders`, et un premier commit Git poussé. Conseil : ne pas sous-estimer le temps de design du modèle dimensionnel et du Hook ; les bâcler coûte deux fois plus de temps en debug ensuite. En cas de retard : sacrifier le DAG de refresh des dimensions et se concentrer sur le pipeline principal de bout en bout.

### 11.2 Analytics, dashboards, observabilité

La deuxième partie est dédiée à l'exposition des données via Metabase, à la qualité du livrable (tests, doc), et éventuellement à la stack d'observabilité Prometheus/Grafana en bonus.

Les étapes recommandées, dans l'ordre. Construire les tables d'agrégation dans le schéma `analytics` en réfléchissant à ce qui est vraiment utile pour les dashboards (`daily_summary`, `seller_daily`, `category_daily`) ; un DAG dédié peut s'en charger. Ajouter Metabase au docker-compose avec un volume nommé pour la persistance. Configurer la connexion Metabase vers PostgreSQL DWH, en faisant attention au host `postgres-dwh` (pas localhost) et au port interne 5432. Créer les deux dashboards minimum (Executive Summary et Top Sellers), en privilégiant la clarté à l'exhaustivité. Écrire les tests pytest (au moins trois) : import sans erreur, `catchup=False`, structure du DAG principal ; les lancer via `docker compose exec airflow-worker pytest tests/ -v`. Finaliser le README avec le schéma Mermaid de l'architecture, les instructions de lancement et les choix techniques justifiés. En bonus si on est en avance : ajouter Prometheus, Grafana et postgres-exporter, et importer le dashboard Grafana d'ID 11010.

Objectifs : deux dashboards Metabase fonctionnels, des tests qui passent, un README complet. Conseil : commencer la doc et les tests en parallèle du code, pas à la fin.

## 12. Critères d'acceptation

### 12.1 Definition of Done

En plus de la documentation commune, spécifiquement pour ce projet : `marketplace_orders_ingest_daily` tourne en succès ; `dwh.fact_orders` contient des données pour au moins une date ; rejouer deux fois le même run donne le même `COUNT(*)` dans `dwh.fact_orders` ; Metabase affiche au moins deux dashboards avec des données réelles ; au moins trois tests pytest passent ; et le README contient un schéma Mermaid et les instructions de lancement.

### 12.2 Tests pytest minimum

Dans `tests/test_dags.py` : une fonction `test_no_import_errors(dagbag)`, une fonction `test_all_dags_catchup_false(dagbag)`, et une fonction `test_marketplace_ingest_has_idempotent_transform(dagbag)`.

### 12.3 Test manuel d'idempotence (que le formateur exécutera)

Lancer une première fois `docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07`, puis compter les lignes avec `docker compose exec postgres-dwh psql -U dwh_user -d dwh -c "SELECT COUNT(*) FROM dwh.fact_orders WHERE dt='2026-04-07';"` (on obtient N lignes). Relancer la même commande de test, puis recompter : on doit toujours obtenir N lignes, et pas 2 × N.

## 13. Observabilité (bonus)

Si le MVP est fini en avance, on ajoute la stack Prometheus, Grafana et exporters. C'est ce qui transforme un projet « ça marche sur ma machine » en projet « production-ready ».

### 13.1 Quoi installer

Les composants pertinents : `prometheus` (le scraper de métriques) ; `grafana` (les dashboards, à mapper sur le port 3001 car le 3000 est pris par Metabase) ; `statsd-exporter` (convertit les métriques StatsD d'Airflow en format Prometheus) ; et `postgres-exporter` (métriques PostgreSQL : connexions, requêtes lentes, locks). Le docker-compose complet et la config Prometheus sont dans la documentation commune.

### 13.2 Activer les métriques Airflow

Ajouter dans le bloc `x-airflow-common` les variables : `AIRFLOW__METRICS__STATSD_ON` à "True", `AIRFLOW__METRICS__STATSD_HOST` à `statsd-exporter`, `AIRFLOW__METRICS__STATSD_PORT` à "9125", et `AIRFLOW__METRICS__STATSD_PREFIX` à `airflow`. Puis relancer avec `docker compose up -d --force-recreate`.

### 13.3 Dashboards Grafana à importer

Dans Grafana (`http://localhost:3001`, login admin/admin), aller dans Dashboards puis Import et coller l'ID. L'ID 11010 (Airflow Cluster) couvre les métriques du scheduler, des tasks et des DAGs. L'ID 9628 (PostgreSQL Database) couvre les connexions, les requêtes et les locks.

## 14. Grille d'évaluation (100 points)

En plus de la documentation commune, spécifiquement pour ce projet : les deux dashboards Metabase comptent pour 10 points dans la rubrique « Pipeline principal fonctionnel » ; les dashboards supplémentaires (bonus Should) valent +5 points ; le DAG de détection d'anomalies (bonus Could) vaut +5 points ; et la stack d'observabilité Prometheus/Grafana (bonus Could) vaut +10 points.

## 15. Pièges spécifiques au projet A

### 15.1 Pièges communs

Voir la documentation commune.

### 15.2 Pièges Metabase

Si on perd les dashboards au `down -v` (tout disparaît), c'est qu'il manque le volume nommé `metabase-data` dans le docker-compose. Si la connexion à PostgreSQL échoue (« Cannot connect »), c'est que le host doit être `postgres-dwh` (nom Docker) et le port interne 5432. Si le premier démarrage est lent (environ 60 secondes de setup), c'est normal : Metabase initialise sa base H2. Si on a un conflit de port avec Grafana (« Bind failed » sur 3000), il faut mapper Grafana sur 3001:3000.

### 15.3 Pièges Pipeline ELT

Si on a des doublons à chaque run, c'est qu'on a fait un INSERT sans DELETE préalable : utiliser le pattern DELETE + INSERT par partition `dt`. Si le run ignore `{{ ds }}`, c'est qu'une date est hardcodée dans le SQL : utiliser les macros Jinja d'Airflow (`{{ ds }}`, `{{ ds_nodash }}`). Si le token API est en dur dans le code (détecté en code review), il faut toujours passer par `Connection.password`. Si `staging.orders` grossit indéfiniment et que le disque se remplit, il faut nettoyer périodiquement par truncate ou partition par `dt`.

## 16. Ressources

### 16.1 Documentation

Metabase Docker, Metabase SQL Questions, Metabase Dashboards, Airflow PostgresHook, Airflow S3Hook, et la documentation PostgreSQL 18.

### 16.2 Bonnes pratiques

Une introduction au Kimball Dimensional Modeling, les DAG Best Practices d'Astronomer, et la documentation Airflow sur les macros et templates Jinja.

### 16.3 Outils Docker et debug

`lazydocker` (une TUI pour Docker), `ctop` (un top pour les conteneurs), et `pgcli` (un client PostgreSQL avec autocomplétion).

### 16.4 Pour aller plus loin

dbt Core (une alternative au SQL Airflow pour les transformations), Apache Superset (une alternative à Metabase, plus puissante mais plus complexe), et Great Expectations (un framework de data quality plus avancé que le `DataQualityOperator`).

## 17. FAQ

**Peut-on utiliser Superset à la place de Metabase ?** Oui, à ses risques. Superset est plus puissant mais plus complexe. Documenter le choix dans le README. Pas de pénalité, pas de bonus.

**Si on n'a pas le temps pour les deux dashboards Metabase, peut-on skipper ?** Un seul dashboard avec trois visualisations est acceptable comme MVP minimal, mais c'est risqué et ça pèsera sur la note.

**Le DataQualityOperator est-il obligatoire ?** Non, c'est un bonus (Should). Se concentrer sur le MVP d'abord.

**Peut-on ajouter dbt pour les transformations ?** Hors scope pour 14 heures. Le mentionner dans le README comme « ce qu'on ferait avec plus de temps ».

**Le bonus observabilité (Prometheus/Grafana) est-il réaliste en 14 heures ?** Si le MVP est fini à 14h le jour 2, oui : environ 2 heures pour ajouter la stack et importer le dashboard 11010. Si on galère sur le MVP, ne pas y aller.

**Comment accéder à PostgreSQL DWH depuis l'extérieur du conteneur ?** Le port 5433 est exposé sur l'hôte. Depuis sa machine : `psql -h localhost -p 5433 -U dwh_user -d dwh`. Depuis un autre conteneur : `psql -h postgres-dwh -p 5432 ...`.

**Que faire si MinIO n'a pas créé le bucket ?** Aller dans la console MinIO (`http://localhost:9001`, login `minio_admin` / `minio_password_2026`) et créer manuellement le bucket `data-lake`, ou exécuter `docker compose restart minio-init`.

Objectif final : un pipeline simple qui tourne, idempotent, avec deux dashboards Metabase lisibles.
