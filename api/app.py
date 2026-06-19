"""
API Marketplace simulée — Maelys Marketplace

Données déterministes : même date → mêmes commandes (seed = md5(date)).
Référentiels stables : vendeurs/produits/clients générés avec un seed global fixe.

Endpoints :
    GET /health                          → pas d'auth, vérification de vie
    GET /orders?date=YYYY-MM-DD          → commandes du jour (auth requise)
    GET /sellers[?limit=N]               → liste des vendeurs (auth requise)
    GET /products                        → catalogue produits (auth requise)
    GET /customers[?limit=N]             → liste des clients (auth requise)

Auth : header "Authorization: Bearer <API_TOKEN>"
"""

import hashlib
import os
import random
from datetime import date, timedelta
from functools import lru_cache, wraps

from flask import Flask, abort, jsonify, request

app = Flask(__name__)

# ── Configuration ───────────────────────────────────────────────────────────
API_TOKEN       = os.environ.get("API_TOKEN", "formation-token-2026")
ORDERS_PER_DAY  = int(os.environ.get("ORDERS_PER_DAY", "800"))

GLOBAL_SEED     = 42
N_SELLERS       = 60
N_PRODUCTS      = 200
N_CUSTOMERS     = 500
COMMISSION_RATE = 0.12
REFERENCE_DATE  = date(2026, 4, 1)

# Anomalies volontaires : date → multiplicateur du volume de commandes.
# Toutes produisent CA < 70 % de la moyenne mobile 7 jours → détectables par le DAG d'anomalies.
ANOMALIES = {
    "2026-04-15": 0.50,   # chute -50 % (incident critique)
    "2026-04-25": 0.45,   # chute -55 % (panne paiement simulée)
}

# Vendeurs devenus inactifs à partir d'une date donnée.
# Permet d'alimenter le KPI "vendeurs inactifs > 7 jours" dans Metabase.
SELLER_INACTIVE_FROM = {
    "SELL-0051": date(2026, 4, 18),
    "SELL-0052": date(2026, 4, 19),
    "SELL-0053": date(2026, 4, 20),
    "SELL-0054": date(2026, 4, 21),
    "SELL-0055": date(2026, 4, 20),
    "SELL-0056": date(2026, 4, 22),
    "SELL-0057": date(2026, 4, 17),
    "SELL-0058": date(2026, 4, 16),
}

CATEGORIES = [
    "Mode", "Électronique", "Maison & Déco",
    "Sport & Loisirs", "Beauté & Santé", "Alimentation",
]

PRICE_RANGES = {
    "Mode":            (15.0,  150.0),
    "Électronique":    (20.0,  500.0),
    "Maison & Déco":   (10.0,  200.0),
    "Sport & Loisirs": (15.0,  300.0),
    "Beauté & Santé":  ( 8.0,   80.0),
    "Alimentation":    ( 5.0,   50.0),
}

COUNTRIES        = ["France", "Belgique", "Espagne", "Italie", "Allemagne", "Portugal"]
COUNTRY_WEIGHTS  = [60, 12, 10, 8, 6, 4]

FRENCH_CITIES = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice",
    "Nantes", "Montpellier", "Strasbourg", "Bordeaux", "Lille",
    "Rennes", "Reims", "Saint-Étienne", "Toulon", "Grenoble",
    "Dijon", "Angers", "Nîmes", "Villeurbanne", "Le Mans",
]

SELLER_PREFIXES  = ["Atelier", "Maison", "Boutique", "Studio", "Les Créations", "La Belle"]
SELLER_SURNAMES  = [
    "Dubois", "Martin", "Bernard", "Petit", "Robert", "Richard",
    "Durand", "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre",
    "Michel", "Garcia", "David", "Bertrand", "Roux", "Vincent",
    "Fournier", "Morel", "Girard", "André", "Mercier", "Dupont",
]

PRODUCT_NAMES = {
    "Mode": [
        "Robe florale", "Jean slim", "Veste en cuir", "Pull cachemire",
        "Chemise oxford", "Manteau long", "Cardigan laine", "Blouse en soie",
        "Pantalon chino", "Sneakers blanches", "Sac à main", "Foulard soie",
        "Ceinture cuir", "Chapeau paille", "Robe bohème",
    ],
    "Électronique": [
        "Casque audio sans fil", "Écouteurs Bluetooth", "Enceinte portable",
        "Chargeur USB-C", "Souris sans fil", "Clavier mécanique",
        "Webcam HD", "Hub USB", "Lampe LED connectée", "Batterie externe",
        "Adaptateur multiport", "Montre connectée", "Tablette 10\"",
        "Support téléphone", "Câble tressé",
    ],
    "Maison & Déco": [
        "Bougie parfumée", "Coussin décoratif", "Vase en céramique",
        "Cadre photo", "Miroir mural", "Plante artificielle", "Plateau en bois",
        "Lampe de table", "Tapis berbère", "Rideau lin",
        "Panier tressé", "Réveil vintage", "Porte-manteau", "Étagère murale",
        "Set de bougies",
    ],
    "Sport & Loisirs": [
        "Tapis de yoga", "Gourde isotherme", "Élastique de résistance",
        "Raquette de tennis", "Ballon de football", "Sac de sport",
        "Puzzle 1000 pièces", "Jeu de cartes premium", "Tente de camping",
        "Sac de couchage", "Boussole", "Lampe frontale",
        "Roller urbain", "Jump rope", "Carnet de bord",
    ],
    "Beauté & Santé": [
        "Crème hydratante", "Sérum vitamine C", "Huile essentielle lavande",
        "Masque visage", "Baume à lèvres", "Shampoing naturel",
        "Savon artisanal", "Brosse bambou", "Diffuseur d'huiles",
        "Coffret soins", "Rouge à lèvres", "Fond de teint",
        "Palette maquillage", "Spray fixateur", "Démaquillant doux",
    ],
    "Alimentation": [
        "Café en grains", "Thé bio assortiment", "Miel artisanal",
        "Huile d'olive extra vierge", "Vinaigre balsamique", "Confiture maison",
        "Chocolat noir 70%", "Tisane relaxante", "Épices du monde",
        "Sauce pimentée", "Pâtes artisanales", "Riz basmati bio",
        "Granola maison", "Biscuits artisanaux", "Sirop d'érable",
    ],
}

QUANTITY_VALUES  = [1, 2, 3, 4, 5]
QUANTITY_WEIGHTS = [55, 25, 12, 5, 3]

STATUS_VALUES  = ["completed", "cancelled", "pending"]
STATUS_WEIGHTS = [85, 10, 5]


# ── Auth ────────────────────────────────────────────────────────────────────
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {API_TOKEN}":
            abort(401)
        return f(*args, **kwargs)
    return decorated


# ── Seed ────────────────────────────────────────────────────────────────────
def seed_from_date(date_str: str) -> int:
    # md5 donne un entier sur 128 bits → on le tronque à 32 bits pour random.seed()
    return int(hashlib.md5(date_str.encode()).hexdigest(), 16) % (2 ** 32)


# ── Helpers ─────────────────────────────────────────────────────────────────
def _nice_price(value: float, rng: random.Random) -> float:
    """Arrondit un prix à une terminaison réaliste (.90, .99, .00, .50, .95).
    Ex : 47.3 → 47.99  |  132.8 → 132.90
    """
    endings = [0.90, 0.99, 0.00, 0.50, 0.95]
    return round(int(value) + rng.choice(endings), 2)


# ── Référentiels (stables, seed global fixe) ────────────────────────────────
@lru_cache(maxsize=1)
def generate_sellers() -> list[dict]:
    # lru_cache(maxsize=1) : calculé une seule fois au premier appel, puis servi depuis le cache.
    # Indispensable ici : les seller_ids doivent être identiques à chaque requête (intégrité référentielle).
    rng = random.Random(GLOBAL_SEED)
    sellers = []
    for i in range(N_SELLERS):
        joined = date(2020, 1, 1) + timedelta(days=rng.randint(0, 365 * 5))
        sellers.append({
            "seller_id":   f"SELL-{i + 1:04d}",
            "name":        f"{rng.choice(SELLER_PREFIXES)} {rng.choice(SELLER_SURNAMES)}",
            "country":     rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0],
            "joined_date": joined.isoformat(),
        })
    return sellers


@lru_cache(maxsize=1)
def generate_products() -> list[dict]:
    rng = random.Random(GLOBAL_SEED + 1)
    sellers = generate_sellers()
    products = []

    # Garantir 1 produit minimum par vendeur : sans ça, un vendeur sans produit
    # provoquerait un fallback coûteux dans _generate_orders et des FK invalides.
    shuffled = list(sellers)
    rng.shuffle(shuffled)
    for seller in shuffled:
        cat = rng.choice(CATEGORIES)
        price_min, price_max = PRICE_RANGES[cat]
        products.append({
            "product_id": f"PROD-{len(products) + 1:04d}",
            "name":       rng.choice(PRODUCT_NAMES[cat]),
            "category":   cat,
            "seller_id":  seller["seller_id"],
            "base_price": _nice_price(rng.uniform(price_min, price_max), rng),
        })

    # Produits restants assignés aléatoirement
    for _ in range(N_PRODUCTS - N_SELLERS):
        cat = rng.choice(CATEGORIES)
        seller = rng.choice(sellers)
        price_min, price_max = PRICE_RANGES[cat]
        products.append({
            "product_id": f"PROD-{len(products) + 1:04d}",
            "name":       rng.choice(PRODUCT_NAMES[cat]),
            "category":   cat,
            "seller_id":  seller["seller_id"],
            "base_price": _nice_price(rng.uniform(price_min, price_max), rng),
        })

    return products


@lru_cache(maxsize=1)
def generate_customers() -> list[dict]:
    rng = random.Random(GLOBAL_SEED + 2)
    customers = []
    for i in range(N_CUSTOMERS):
        signup = date(2018, 1, 1) + timedelta(days=rng.randint(0, 365 * 7))
        customers.append({
            "customer_id": f"CUST-{i + 1:04d}",
            "email":       f"client{i + 1:04d}@example.com",
            "city":        rng.choice(FRENCH_CITIES),
            "signup_date": signup.isoformat(),
        })
    return customers


@lru_cache(maxsize=1)
def _seller_pareto_weights() -> list[float]:
    """Poids Pareto stables pour la distribution des ventes par vendeur.

    alpha=1.5 : longue traîne prononcée → ~15 vendeurs captent l'essentiel du CA,
    une quinzaine de petits vendeurs restent sans vente sur 7 jours → KPI 'inactifs' non vide.
    Distribution uniforme donnerait ~13 ventes/vendeur/jour, rendant le KPI inutile.
    """
    rng = random.Random(GLOBAL_SEED + 3)
    raw = [rng.paretovariate(1.5) for _ in range(N_SELLERS)]
    total = sum(raw)
    return [w / total for w in raw]


@lru_cache(maxsize=1)
def _products_by_seller() -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for p in generate_products():
        result.setdefault(p["seller_id"], []).append(p)
    return result


# ── Nombre de commandes par date (formule déterministe) ─────────────────────
def _order_count_for(date_str: str) -> int:
    d = date.fromisoformat(date_str)
    count = ORDERS_PER_DAY

    if date_str in ANOMALIES:
        return int(count * ANOMALIES[date_str])

    # Bonus week-end
    if d.weekday() == 5:    # Samedi +25 %
        count = int(count * 1.25)
    elif d.weekday() == 6:  # Dimanche +20 %
        count = int(count * 1.20)

    # Tendance haussière : +1 %/jour depuis REFERENCE_DATE, plafonnée à +30 %
    days_delta = (d - REFERENCE_DATE).days
    if days_delta > 0:
        count = int(count * min(1.0 + days_delta * 0.01, 1.30))

    return count


# ── Génération des commandes (résultat mis en cache par date) ────────────────
@lru_cache(maxsize=60)
def _generate_orders(date_str: str) -> list[dict]:
    # maxsize=60 : cache les 60 dernières dates appelées.
    # La même date appelée deux fois (test d'idempotence) retourne exactement le même résultat
    # sans recalcul — c'est ce qui prouve le déterminisme côté API.
    rng        = random.Random(seed_from_date(date_str))
    current    = date.fromisoformat(date_str)
    sellers    = [s for s in generate_sellers()
                  if s["seller_id"] not in SELLER_INACTIVE_FROM
                  or current < SELLER_INACTIVE_FROM[s["seller_id"]]]
    all_weights = _seller_pareto_weights()
    all_sellers = generate_sellers()
    weights    = [all_weights[i] for i, s in enumerate(all_sellers) if s in sellers]
    customers  = generate_customers()
    by_seller  = _products_by_seller()

    orders = []
    for i in range(_order_count_for(date_str)):
        seller   = rng.choices(sellers, weights=weights, k=1)[0]
        products = by_seller.get(seller["seller_id"], generate_products())
        product  = rng.choice(products)
        customer = rng.choice(customers)

        quantity     = rng.choices(QUANTITY_VALUES, weights=QUANTITY_WEIGHTS, k=1)[0]
        # ±5 % autour du base_price : simule des promotions légères ou variations de marge
        unit_price   = round(product["base_price"] * rng.uniform(0.95, 1.05), 2)
        total_amount = round(unit_price * quantity, 2)
        # Commission calculée sur toutes les commandes (y compris cancelled/pending).
        # C'est le pipeline qui filtre sur status='completed' pour le CA réel.
        commission   = round(total_amount * COMMISSION_RATE, 2)
        status       = rng.choices(STATUS_VALUES, weights=STATUS_WEIGHTS, k=1)[0]

        h = rng.randint(6, 22)
        m = rng.randint(0, 59)
        s = rng.randint(0, 59)

        orders.append({
            "order_id":     f"ORD-{date_str.replace('-', '')}-{i + 1:06d}",
            "dt":           date_str,
            "created_at":   f"{date_str}T{h:02d}:{m:02d}:{s:02d}Z",
            "seller_id":    seller["seller_id"],
            "product_id":   product["product_id"],
            "customer_id":  customer["customer_id"],
            "quantity":     quantity,
            "unit_price":   unit_price,
            "total_amount": total_amount,
            "commission":   commission,
            "status":       status,
        })
    return orders


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "1.0.0"})


@app.route("/orders")
@require_auth
def get_orders():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "Paramètre 'date' requis (format YYYY-MM-DD)"}), 400
    try:
        date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": "Format de date invalide, attendu YYYY-MM-DD"}), 400

    orders = _generate_orders(date_str)
    return jsonify({"date": date_str, "count": len(orders), "orders": orders})


@app.route("/sellers")
@require_auth
def get_sellers():
    limit   = request.args.get("limit", type=int)
    sellers = generate_sellers()
    result  = sellers[:limit] if limit else sellers
    return jsonify({"count": len(result), "sellers": result})


@app.route("/products")
@require_auth
def get_products():
    products = generate_products()
    return jsonify({"count": len(products), "products": products})


@app.route("/customers")
@require_auth
def get_customers():
    limit     = request.args.get("limit", type=int)
    customers = generate_customers()
    result    = customers[:limit] if limit else customers
    return jsonify({"count": len(result), "customers": result})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
