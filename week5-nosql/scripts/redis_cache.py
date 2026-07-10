#!/usr/bin/env python3
"""
SolarMboa Technologies – Semaine 5 : Redis
==========================================
Cas d'usage :
  - Cache LRU des 5 000 compteurs IoT les plus actifs
  - TTL adapté au cycle de facturation journalier (24h)
  - File de commandes activation/désactivation (DB 1)
  - Benchmark latence cache vs lecture directe CSV

Usage : python3 redis_cache.py
"""

import redis
import csv
import json
import time
import random
from datetime import datetime
from collections import defaultdict

# ── Connexion ────────────────────────────────────────────────────
R_CACHE = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
R_QUEUES = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

DATA_FILE = "data/sensors_telemetry.csv"
TOP_N = 5000  # Compteurs les plus actifs à mettre en cache
TTL_SECS = 86400  # 24 h – cycle de facturation journalier

# ── Normalisation région (problème qualité connu) ────────────────
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


def normalize_region(raw: str) -> str:
    if not raw:
        return "Inconnu"
    return REGION_MAP.get(raw.strip().lower(), raw.strip().title())


def normalize_timestamp(raw: str) -> str:
    """Gère les 3 formats de timestamp présents dans les données."""
    raw = raw.strip()
    # Unix epoch
    if raw.isdigit():
        return datetime.utcfromtimestamp(int(raw)).isoformat()
    # DD/MM/YYYY ou DD-MM-YYYY
    for fmt in ("%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).isoformat()
        except ValueError:
            pass
    # ISO 8601 – déjà OK
    return raw


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1 – Identifier les TOP_N capteurs les plus actifs
# ════════════════════════════════════════════════════════════════
def get_top_sensors(filepath: str, top_n: int) -> list[str]:
    print(f"\n[1/4] Calcul des {top_n} capteurs les plus actifs...")
    counts: dict[str, int] = defaultdict(int)
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            counts[row["sensor_id"]] += 1
    top = sorted(counts, key=counts.get, reverse=True)[:top_n]
    print(f"      → {len(counts)} capteurs uniques trouvés. Top {top_n} sélectionnés.")
    return top


# ════════════════════════════════════════════════════════════════
# ÉTAPE 2 – Charger les derniers états dans le cache Redis
# ════════════════════════════════════════════════════════════════
def load_cache(filepath: str, top_sensors: set[str]) -> int:
    print(f"\n[2/4] Chargement du cache Redis (DB 0)...")
    latest: dict[str, dict] = {}

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row["sensor_id"]
            if sid in top_sensors:
                # On garde toujours la mesure la plus récente
                existing = latest.get(sid)
                if existing is None or row["timestamp"] > existing["timestamp"]:
                    latest[sid] = row

    pipe = R_CACHE.pipeline(transaction=False)
    for sid, row in latest.items():
        key = f"sensor:state:{sid}"
        payload = {
            "sensor_id": sid,
            "installation_id": row["installation_id"],
            "timestamp": normalize_timestamp(row["timestamp"]),
            "solar_output_w": row["solar_output_w"] or "null",
            "battery_pct": row["battery_level_pct"],
            "consumption_w": row["consumption_w"],
            "alert_code": row.get("alert_code", "").upper() or "NONE",
            "region": normalize_region(row["region"]),
            "cached_at": datetime.utcnow().isoformat(),
        }
        pipe.hset(key, mapping=payload)
        pipe.expire(key, TTL_SECS)

    pipe.execute()
    count = len(latest)
    print(f"      → {count} états de capteurs chargés avec TTL={TTL_SECS}s (24h)")
    return count


# ════════════════════════════════════════════════════════════════
# ÉTAPE 3 – File de commandes activation/désactivation (DB 1)
# ════════════════════════════════════════════════════════════════
def seed_command_queue(top_sensors: list[str], n: int = 50) -> None:
    print(f"\n[3/4] Seed de {n} commandes dans la file (DB 1)...")
    sample = random.sample(top_sensors, min(n, len(top_sensors)))
    pipe = R_QUEUES.pipeline()
    for sid in sample:
        cmd = {
            "sensor_id": sid,
            "action": random.choice(["ACTIVATE", "DEACTIVATE", "RESTART"]),
            "issued_at": datetime.utcnow().isoformat(),
            "priority": random.choice(["HIGH", "NORMAL"]),
        }
        # LPUSH = priorité HIGH en tête de liste
        if cmd["priority"] == "HIGH":
            pipe.lpush("commands:queue", json.dumps(cmd))
        else:
            pipe.rpush("commands:queue", json.dumps(cmd))
    pipe.execute()
    qlen = R_QUEUES.llen("commands:queue")
    print(f"      → File 'commands:queue' contient {qlen} commandes.")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 4 – Benchmark : cache Redis vs lecture CSV brute
# ════════════════════════════════════════════════════════════════
def benchmark(top_sensors: list[str], n_reads: int = 1000) -> None:
    print(f"\n[4/4] Benchmark latence ({n_reads} lectures)...")
    sample_ids = random.choices(top_sensors[:1000], k=n_reads)

    # -- Latence Redis --
    t0 = time.perf_counter()
    pipe = R_CACHE.pipeline(transaction=False)
    for sid in sample_ids:
        pipe.hgetall(f"sensor:state:{sid}")
    results = pipe.execute()
    redis_ms = (time.perf_counter() - t0) * 1000

    # -- Latence CSV (simulation lecture séquentielle brute) --
    lookup = {sid: None for sid in sample_ids}
    t0 = time.perf_counter()
    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["sensor_id"] in lookup:
                lookup[row["sensor_id"]] = row
    csv_ms = (time.perf_counter() - t0) * 1000

    print(f"\n  ┌─────────────────────────────────────────────┐")
    print(f"  │  BENCHMARK – {n_reads} lectures aléatoires         │")
    print(f"  ├─────────────────────────────────────────────┤")
    print(f"  │  Redis cache (pipeline) : {redis_ms:>8.1f} ms       │")
    print(f"  │  CSV scan séquentiel    : {csv_ms:>8.1f} ms       │")
    print(f"  │  Gain de vitesse        : {csv_ms/redis_ms:>8.1f}x         │")
    print(f"  └─────────────────────────────────────────────┘")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 5 – Vérifications et stats
# ════════════════════════════════════════════════════════════════
def print_stats() -> None:
    print("\n[INFO] Statistiques Redis :")
    info = R_CACHE.info("memory")
    keys = R_CACHE.dbsize()
    sample_key = R_CACHE.randomkey()
    ttl_left = R_CACHE.ttl(sample_key) if sample_key else -1

    print(f"  Clés en cache (DB 0)     : {keys}")
    print(f"  Mémoire utilisée         : {info['used_memory_human']}")
    print(f"  TTL restant (clé sample) : {ttl_left}s")
    print(f"  Commandes en queue (DB1) : {R_QUEUES.llen('commands:queue')}")

    # Exemple de lecture d'un état complet
    print("\n[INFO] Exemple d'état capteur depuis le cache :")
    sample = R_CACHE.hgetall(sample_key)
    for k, v in sample.items():
        print(f"  {k:<20} : {v}")


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  SolarMboa – Redis Loader (Semaine 5 NoSQL)")
    print("=" * 60)

    # Vérifier la connexion
    try:
        R_CACHE.ping()
        print("  Connexion Redis OK\n")
    except redis.ConnectionError as e:
        print(f"  ERREUR : Redis non accessible – {e}")
        print("  Assurez-vous que Docker est lancé : docker compose up -d redis")
        exit(1)

    # Flush pour démo propre
    R_CACHE.flushdb()
    R_QUEUES.flushdb()

    top_sensors = get_top_sensors(DATA_FILE, TOP_N)
    top_set = set(top_sensors)
    loaded = load_cache(DATA_FILE, top_set)
    seed_command_queue(top_sensors)
    benchmark(top_sensors)
    print_stats()

    print("\n[OK] Redis chargé avec succès.")
