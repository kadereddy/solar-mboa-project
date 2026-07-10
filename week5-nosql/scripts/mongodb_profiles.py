#!/usr/bin/env python3
"""
SolarMboa Technologies – Semaine 5 : MongoDB
=============================================
Cas d'usage :
  - Profils d'installation enrichis (schéma variable selon type client)
  - Index géospatiaux pour requêtes de proximité
  - Pipelines d'agrégation : revenu moyen par type & par région

Usage : python3 mongodb_profiles.py
"""

import json
import os
import random
from datetime import datetime, date, timedelta
from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import BulkWriteError

# ── Connexion ────────────────────────────────────────────────────
MONGO_URI = f"mongodb://{os.getenv('MONGO_USER','solarmboa_app')}:{os.getenv('MONGO_PASSWORD','app_password_2026')}@localhost:27017/solarmboa?authSource=admin"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data")


# ── Données de référence interventions (simulées) ────────────────
VISIT_TYPES = ["preventive", "corrective", "emergency", "installation"]
ALERT_REASONS = [
    "Batterie faible",
    "Surconsommation",
    "Panne panneau",
    "Câble endommagé",
    "Firmware obsolète",
]

REGION_MAP = {
    "lit.": "Littoral",
    "ltl": "Littoral",
    "littoral": "Littoral",
    "ctr": "Centre",
    "centre": "Centre",
    "oue": "Ouest",
    "ouest-cm": "Ouest",
    "ouest": "Ouest",
    "no": "Nord-Ouest",
    "nord_ouest": "Nord-Ouest",
    "nord-ouest": "Nord-Ouest",
    "ada": "Adamaoua",
    "adamaoua": "Adamaoua",
    "adamawa": "Adamaoua",
    "est": "Est",
    "e": "Est",
    "est-cm": "Est",
    "en": "Extrême-Nord",
    "extreme-nord": "Extrême-Nord",
    "ext-nord": "Extrême-Nord",
}


# ── Normalisation dates ──────────────────────────────────────────
def parse_date(raw: str) -> str | None:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1 – Enrichissement des documents par type de client
# Chaque type a un sous-document spécifique (schéma variable)
# ════════════════════════════════════════════════════════════════
def enrich_document(inst: dict) -> dict:
    """Ajoute les champs spécifiques au type de client + GeoJSON."""
    doc = inst.copy()

    # Normalisation date
    doc["install_date"] = parse_date(str(inst.get("install_date", "")))

    # GeoJSON Point pour index géospatial (skippé si GPS null)
    if inst.get("gps_lat") and inst.get("gps_lon"):
        doc["location"] = {
            "type": "Point",
            "coordinates": [inst["gps_lon"], inst["gps_lat"]],  # GeoJSON: [lon, lat]
        }
    else:
        doc["location"] = None

    # Suppression des champs GPS bruts (remplacés par GeoJSON)
    doc.pop("gps_lat", None)
    doc.pop("gps_lon", None)

    # ── Sous-documents spécifiques par type ──
    ct = inst.get("client_type")

    if ct == "residential":
        doc["equipment"] = {
            "panel_capacity_wp": inst.get("panel_capacity_wp"),
            "battery_capacity_wh": inst.get("battery_capacity_wh"),
            "num_appliances": inst.get("num_appliances"),
            "appliance_types": random.sample(
                [
                    "ampoules",
                    "TV",
                    "ventilateur",
                    "réfrigérateur",
                    "chargeurs",
                    "pompe",
                ],
                min(inst.get("num_appliances", 2) or 2, 6),
            ),
            "inverter_type": random.choice(
                ["Victron 1kVA", "Studer 800W", "Generic 500W"]
            ),
        }
        doc["contract"] = {
            "tariff_plan": inst.get("tariff_plan"),
            "monthly_quota_kwh": random.randint(20, 80),
            "payment_method": random.choice(["mtn_momo", "orange_money"]),
        }

    elif ct == "sme":
        doc["equipment"] = {
            "panel_capacity_wp": inst.get("panel_capacity_wp"),
            "battery_capacity_wh": inst.get("battery_capacity_wh"),
            "num_appliances": inst.get("num_appliances"),
            "business_type": random.choice(
                [
                    "boutique",
                    "restaurant",
                    "atelier couture",
                    "pharmacie",
                    "cyber café",
                    "menuiserie",
                ]
            ),
            "peak_hours": {"start": "08:00", "end": "20:00"},
        }
        doc["contract"] = {
            "tariff_plan": inst.get("tariff_plan"),
            "monthly_quota_kwh": random.randint(100, 500),
            "payment_method": random.choice(
                ["mtn_momo", "orange_money", "bank_transfer"]
            ),
            "sla_uptime_pct": random.choice([95, 98, 99]),
        }

    elif ct == "health_center":
        doc["equipment"] = {
            "panel_capacity_wp": inst.get("panel_capacity_wp"),
            "battery_capacity_wh": inst.get("battery_capacity_wh"),
            "num_appliances": inst.get("num_appliances"),
            "critical_devices": random.sample(
                [
                    "réfrigérateur vaccins",
                    "lampe bloc opératoire",
                    "centrifugeuse",
                    "microscope",
                ],
                2,
            ),
            "backup_autonomy_h": random.choice([8, 12, 24, 48]),
        }
        doc["contract"] = {
            "tariff_plan": inst.get("tariff_plan"),
            "monthly_quota_kwh": random.randint(200, 800),
            "payment_method": "bank_transfer",
            "priority_sla": True,
            "emergency_contact": f"+237{random.randint(600000000,699999999)}",
        }

    elif ct == "school":
        doc["equipment"] = {
            "panel_capacity_wp": inst.get("panel_capacity_wp"),
            "battery_capacity_wh": inst.get("battery_capacity_wh"),
            "num_appliances": inst.get("num_appliances"),
            "num_classrooms_powered": random.randint(2, 12),
            "has_computer_lab": random.choice([True, False]),
        }
        doc["contract"] = {
            "tariff_plan": inst.get("tariff_plan"),
            "monthly_quota_kwh": random.randint(50, 300),
            "payment_method": random.choice(["bank_transfer", "cash"]),
            "academic_schedule": {"school_days": ["Mon", "Tue", "Wed", "Thu", "Fri"]},
        }

    # ── Historique d'interventions (1 à 4 visites simulées) ──
    num_visits = random.randint(1, 4)
    install_dt = datetime.strptime(
        parse_date(doc["install_date"]) or "2022-01-01", "%Y-%m-%d"
    )
    doc["interventions_history"] = []
    for i in range(num_visits):
        days_after = random.randint(30, 800)
        visit_dt = install_dt + timedelta(days=days_after)
        if visit_dt > datetime.now():
            break
        doc["interventions_history"].append(
            {
                "visit_date": visit_dt.strftime("%Y-%m-%d"),
                "visit_type": random.choice(VISIT_TYPES),
                "technician_id": f"TECH-{random.randint(1, 210):04d}",
                "duration_min": random.randint(30, 240),
                "notes": random.choice(ALERT_REASONS),
                "resolved": random.choice([True, True, True, False]),
            }
        )

    doc["metadata"] = {
        "created_at": datetime.utcnow().isoformat(),
        "data_source": "DHI_Academy_Projet2",
        "schema_ver": "1.0",
    }

    return doc


# ════════════════════════════════════════════════════════════════
# ÉTAPE 2 – Chargement dans MongoDB
# ════════════════════════════════════════════════════════════════
def load_installations(db) -> int:
    print("\n[1/3] Chargement des installations dans MongoDB...")
    with open(f"{DATA_DIR}/installations.json", encoding="utf-8") as f:
        raw_data = json.load(f)

    documents = [enrich_document(inst) for inst in raw_data]

    # Upsert pour idempotence
    col = db["installations"]
    col.drop()
    try:
        result = col.insert_many(documents, ordered=False)
        print(f"      → {len(result.inserted_ids)} documents insérés")
    except BulkWriteError as e:
        print(
            f"      → BulkWriteError : {e.details['nInserted']} insérés, {len(e.details['writeErrors'])} erreurs"
        )

    # Recréer les index après drop
    col.create_index([("location", GEOSPHERE)])
    col.create_index([("installation_id", 1)], unique=True)
    col.create_index([("region", 1), ("status", 1)])
    col.create_index([("client_type", 1)])
    col.create_index([("tariff_plan", 1)])
    print("      → Index géospatiaux et standards recréés")
    return len(documents)


# ════════════════════════════════════════════════════════════════
# ÉTAPE 3 – Pipelines d'agrégation métier
# ════════════════════════════════════════════════════════════════
def run_aggregations(db) -> None:
    col = db["installations"]

    # ── Agrégation 1 : Nombre d'installations par type & statut ──
    print("\n[2/3] Pipeline 1 : Distribution par type de client & statut")
    pipeline_type = [
        {
            "$group": {
                "_id": {"type": "$client_type", "status": "$status"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.type": 1, "_id.status": 1}},
    ]
    results = list(col.aggregate(pipeline_type))
    print(f"  {'Type':<15} {'Statut':<12} {'Nb':>6}")
    print(f"  {'-'*35}")
    for r in results:
        print(f"  {r['_id']['type']:<15} {r['_id']['status']:<12} {r['count']:>6}")

    # ── Agrégation 2 : Distribution par région ──
    print("\n[2/3] Pipeline 2 : Installations actives par région")
    pipeline_region = [
        {"$match": {"status": "active"}},
        {
            "$group": {
                "_id": "$region",
                "total": {"$sum": 1},
                "types": {"$addToSet": "$client_type"},
            }
        },
        {"$sort": {"total": -1}},
    ]
    results2 = list(col.aggregate(pipeline_region))
    print(f"  {'Région':<20} {'Actives':>8}")
    print(f"  {'-'*30}")
    for r in results2:
        print(f"  {str(r['_id']):<20} {r['total']:>8}")

    # ── Agrégation 3 : Capacité moyenne par type de client ──
    print("\n[2/3] Pipeline 3 : Capacité panneaux (Wp) moyenne par type")
    pipeline_capacity = [
        {
            "$group": {
                "_id": "$client_type",
                "avg_panel_wp": {"$avg": "$panel_capacity_wp"},
                "avg_battery_wh": {"$avg": "$battery_capacity_wh"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"avg_panel_wp": -1}},
    ]
    results3 = list(col.aggregate(pipeline_capacity))
    print(
        f"  {'Type':<15} {'Moy. Panneaux (Wp)':>20} {'Moy. Batterie (Wh)':>20} {'N':>6}"
    )
    print(f"  {'-'*65}")
    for r in results3:
        avg_p = r["avg_panel_wp"] or 0
        avg_b = r["avg_battery_wh"] or 0
        print(f"  {str(r['_id']):<15} {avg_p:>20.0f} {avg_b:>20.0f} {r['count']:>6}")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 4 – Requête géospatiale demo
# ════════════════════════════════════════════════════════════════
def geo_query_demo(db) -> None:
    col = db["installations"]
    print(
        "\n[3/3] Requête géospatiale : installations dans un rayon de 50 km de Douala"
    )
    # Douala : lon=9.7085, lat=4.0511
    nearby = list(
        col.find(
            {
                "location": {
                    "$near": {
                        "$geometry": {"type": "Point", "coordinates": [9.7085, 4.0511]},
                        "$maxDistance": 50_000,  # 50 km en mètres
                    }
                }
            },
            {"client_name": 1, "client_type": 1, "region": 1, "status": 1, "_id": 0},
        ).limit(10)
    )

    print(
        f"  → {len(nearby)} installations trouvées dans les 50 km autour de Douala (top 10 affichées)"
    )
    for inst in nearby:
        print(
            f"    • [{inst['client_type']:>12}] {inst['client_name']} – {inst['region']} ({inst['status']})"
        )


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  SolarMboa – MongoDB Loader (Semaine 5 NoSQL)")
    print("=" * 60)

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["solarmboa"]
        print("  Connexion MongoDB OK\n")
    except Exception as e:
        print(f"  ERREUR : MongoDB non accessible – {e}")
        print("  Assurez-vous que Docker est lancé : docker compose up -d mongodb")
        exit(1)

    load_installations(db)
    run_aggregations(db)
    geo_query_demo(db)

    print("\n[OK] MongoDB chargé avec succès.")
    client.close()
