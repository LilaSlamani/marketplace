"""
Script de test rapide du MarketplaceAPIHook.
Lance ce script depuis le conteneur airflow-worker APRÈS avoir créé la Connection.

Usage :
    docker compose exec airflow-worker python /opt/airflow/dags/../plugins/hooks/test_hook.py

Ou depuis le répertoire du projet :
    docker compose exec airflow-worker python /opt/airflow/scripts/test_hook.py
"""

import sys
sys.path.insert(0, "/opt/airflow/plugins")

from hooks.marketplace_hook import MarketplaceAPIHook

hook = MarketplaceAPIHook()

print("\n--- Test get_sellers() ---")
sellers = hook.get_sellers()
print(f"Nombre de vendeurs : {len(sellers)}")
print(f"Premier vendeur    : {sellers[0]}")

print("\n--- Test get_products() ---")
products = hook.get_products()
print(f"Nombre de produits : {len(products)}")
print(f"Premier produit    : {products[0]}")

print("\n--- Test get_customers() ---")
customers = hook.get_customers()
print(f"Nombre de clients  : {len(customers)}")
print(f"Premier client     : {customers[0]}")

print("\n--- Test get_orders() ---")
orders = hook.get_orders("2026-04-07")
print(f"Nombre de commandes : {len(orders)}")
print(f"Première commande   : {orders[0]}")

print("\n--- Test anomalie (2026-04-15) ---")
orders_anomalie = hook.get_orders("2026-04-15")
print(f"Commandes le jour de l'anomalie : {len(orders_anomalie)} (attendu ~400)")

print("\n✓ Tous les tests passent.")
