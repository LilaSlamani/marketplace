-- ============================================================
-- Maelys Marketplace — Data Warehouse Schema
-- Modélisation en étoile (Kimball)
-- Chargé automatiquement au démarrage de postgres-dwh
-- ============================================================

-- ── Schémas ───────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;


-- ============================================================
-- STAGING — copie fidèle de l'API, aucune transformation
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id    VARCHAR         NOT NULL,
    dt          DATE            NOT NULL,
    created_at  TIMESTAMP       NOT NULL,
    seller_id   VARCHAR         NOT NULL,
    product_id  VARCHAR         NOT NULL,
    customer_id VARCHAR         NOT NULL,
    quantity    INT             NOT NULL,
    unit_price  NUMERIC(10, 2)  NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    commission  NUMERIC(10, 2)  NOT NULL,
    status      VARCHAR         NOT NULL,
    loaded_at   TIMESTAMP       NOT NULL DEFAULT NOW()
);


-- ============================================================
-- DWH — dimensions (à peupler AVANT fact_orders)
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id   VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    country     VARCHAR NOT NULL,
    joined_date DATE    NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id  VARCHAR         PRIMARY KEY,
    name        VARCHAR         NOT NULL,
    category    VARCHAR         NOT NULL,
    base_price  NUMERIC(10, 2)  NOT NULL,
    -- seller_id référence dim_seller : un produit appartient toujours à un vendeur existant
    seller_id   VARCHAR         NOT NULL REFERENCES dwh.dim_seller(seller_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    email       VARCHAR NOT NULL,
    city        VARCHAR NOT NULL,
    signup_date DATE    NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt           DATE    PRIMARY KEY,
    year         INT     NOT NULL,
    month        INT     NOT NULL,
    day_of_week  INT     NOT NULL,  -- 0 = lundi, 6 = dimanche
    -- précalculé pour éviter de recalculer dans chaque dashboard
    is_weekend   BOOLEAN NOT NULL
);


-- ============================================================
-- DWH — table de faits
-- Grain : 1 ligne = 1 commande
-- Rafraîchie par DELETE + INSERT par partition dt (idempotence)
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id     VARCHAR         PRIMARY KEY,
    dt           DATE            NOT NULL REFERENCES dwh.dim_date(dt),
    seller_id    VARCHAR         NOT NULL REFERENCES dwh.dim_seller(seller_id),
    product_id   VARCHAR         NOT NULL REFERENCES dwh.dim_product(product_id),
    customer_id  VARCHAR         NOT NULL REFERENCES dwh.dim_customer(customer_id),
    quantity     INT             NOT NULL,
    unit_price   NUMERIC(10, 2)  NOT NULL,
    total_amount NUMERIC(10, 2)  NOT NULL,
    -- commission stockée brute sur toutes les commandes
    -- le filtre status='completed' est appliqué dans analytics, pas ici
    commission   NUMERIC(10, 2)  NOT NULL,
    status       VARCHAR         NOT NULL  -- 'completed' | 'cancelled' | 'pending'
);


-- ============================================================
-- ANALYTICS — agrégations pré-calculées pour Metabase
-- Rafraîchies par le DAG marketplace_analytics_aggregate_daily
-- ============================================================

-- CA par jour — lu par dashboard Executive Summary
CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt                DATE            PRIMARY KEY,
    total_orders      INT             NOT NULL,  -- toutes commandes (tous statuts)
    completed_orders  INT             NOT NULL,  -- completed uniquement
    total_revenue     NUMERIC(10, 2)  NOT NULL,  -- completed uniquement
    total_commission  NUMERIC(10, 2)  NOT NULL   -- completed uniquement
    -- panier moyen = total_revenue / completed_orders (calculé dans Metabase)
    -- taux annulation = 1 - completed_orders / total_orders (calculé dans Metabase)
);

-- CA par vendeur par jour — lu par dashboard Top Sellers
CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt               DATE            NOT NULL,
    seller_id        VARCHAR         NOT NULL,
    total_orders     INT             NOT NULL,  -- toutes commandes
    completed_orders INT             NOT NULL,  -- completed uniquement
    total_revenue    NUMERIC(10, 2)  NOT NULL,  -- completed uniquement
    PRIMARY KEY (dt, seller_id)
);

-- CA par catégorie par jour — lu par dashboard Catégories
CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt               DATE            NOT NULL,
    category         VARCHAR         NOT NULL,
    total_orders     INT             NOT NULL,  -- toutes commandes
    completed_orders INT             NOT NULL,  -- completed uniquement
    total_revenue    NUMERIC(10, 2)  NOT NULL,  -- completed uniquement
    PRIMARY KEY (dt, category)
);

-- Activité client — lu par dashboard Clients actifs vs dormants
-- 1 ligne par client, mise à jour à chaque run analytics
CREATE TABLE IF NOT EXISTS analytics.customer_activity (
    customer_id      VARCHAR         PRIMARY KEY,
    last_order_date  DATE            NOT NULL,  -- dernière commande completed
    total_orders     INT             NOT NULL,  -- completed uniquement
    total_revenue    NUMERIC(10, 2)  NOT NULL   -- completed uniquement
    -- client actif   : last_order_date >= TODAY - 30j
    -- client dormant : last_order_date <  TODAY - 30j
);

-- Résultats du DAG de détection d'anomalies — lu par dashboard Alertes
CREATE TABLE IF NOT EXISTS analytics.anomalies (
    detected_at   TIMESTAMP       NOT NULL DEFAULT NOW(),
    dt            DATE            NOT NULL,
    scope         VARCHAR         NOT NULL,  -- 'global' ou 'seller'
    seller_id     VARCHAR,                   -- NULL si scope = 'global'
    metric        VARCHAR         NOT NULL,  -- ex: 'total_revenue'
    expected      NUMERIC(10, 2)  NOT NULL,  -- moyenne mobile 7 jours
    actual        NUMERIC(10, 2)  NOT NULL,  -- valeur réelle du jour
    deviation_pct NUMERIC(6, 2)   NOT NULL,  -- (actual - expected) / expected * 100
    severity      VARCHAR         NOT NULL   -- 'warning' (>-30%) | 'critical' (>-50%)
);
