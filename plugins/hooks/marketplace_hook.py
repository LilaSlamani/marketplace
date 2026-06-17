"""
MarketplaceAPIHook — Connecteur Airflow vers l'API Marketplace

Hérite de BaseHook pour lire les credentials depuis une Connection Airflow.
Connection attendue (conn_id='marketplace_api') :
    - schema   : http
    - host     : api-marketplace  (nom Docker du service)
    - port     : 5000
    - password : le token Bearer
"""

from airflow.hooks.base import BaseHook
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MarketplaceAPIHook(BaseHook):

    conn_name_attr    = "marketplace_conn_id"
    default_conn_name = "marketplace_api"
    conn_type         = "http"
    hook_name         = "Marketplace API"

    def __init__(self, marketplace_conn_id: str = default_conn_name):
        super().__init__()
        self.marketplace_conn_id = marketplace_conn_id
        self._session  = None
        self._base_url = None

    def get_conn(self) -> requests.Session:
        """
        Construit et retourne la session HTTP avec auth Bearer.
        Appelé une seule fois — résultat mis en cache dans self._session.
        """
        if self._session is not None:
            return self._session

        conn = self.get_connection(self.marketplace_conn_id)

        # Construit l'URL de base depuis les champs de la Connection Airflow
        scheme         = conn.schema or "http"
        host           = conn.host
        port           = conn.port or 5000
        self._base_url = f"{scheme}://{host}:{port}"

        # Retry automatique sur les erreurs serveur (500/502/503/504) et timeouts
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,          # attend 1s, 2s, 4s entre les essais
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        self._session = requests.Session()
        self._session.mount("http://",  adapter)
        self._session.mount("https://", adapter)

        # Token Bearer lu depuis le champ password de la Connection — jamais en dur
        self._session.headers.update({
            "Authorization": f"Bearer {conn.password}"
        })

        return self._session

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Effectue un GET et lève une exception si le statut HTTP n'est pas 2xx."""
        session  = self.get_conn()
        url      = f"{self._base_url}{endpoint}"
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_orders(self, date: str) -> list[dict]:
        """
        Retourne les commandes pour la date donnée (format YYYY-MM-DD).
        Appelle GET /orders?date=...
        """
        data = self._get("/orders", params={"date": date})
        return data["orders"]

    def get_sellers(self, limit: int = None) -> list[dict]:
        """
        Retourne la liste des vendeurs.
        Appelle GET /sellers?limit=...
        """
        params = {"limit": limit} if limit else {}
        data   = self._get("/sellers", params=params)
        return data["sellers"]

    def get_products(self) -> list[dict]:
        """
        Retourne le catalogue produits.
        Appelle GET /products
        """
        data = self._get("/products")
        return data["products"]

    def get_customers(self, limit: int = None) -> list[dict]:
        """
        Retourne la liste des clients.
        Appelle GET /customers?limit=...
        """
        params = {"limit": limit} if limit else {}
        data   = self._get("/customers", params=params)
        return data["customers"]
