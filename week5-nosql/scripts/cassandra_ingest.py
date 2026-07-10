# Script d'ingestion Cassandra
#!/usr/bin/env python3
"""
SolarMboa Technologies – Semaine 5 : Cassandra
===============================================
Cas d'usage :
  - Ingestion haute fréquence mesures télémétrie (12M événements/jour)
  - Partition key : region + date  →  clustering : sensor_id + timestamp
  - Simulation ingestion 24h et mesure throughput
  - Réplication SimpleStrategy RF=3 (prod : NetworkTopologyStrategy)

Usage : python3 cassandra_ingest.py
"""

import csv
import os
import time
import uuid
from datetime import datetime, date
from collections import defaultdict

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, ConsistencyLevel, BatchType

DATA_FILE = "data/sensors_telemetry.csv"
BATCH_SIZE = 50  # Cassandra : batches petits pour éviter les timeouts

# ── Normalisation région ─────────────────────────────────────────
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


def normalize_timestamp(raw: str) -> datetime:
    raw = raw.strip()
    if raw.isdigit():
        return datetime.utcfromtimestamp(int(raw))
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    return datetime.utcnow()


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1 – Création du keyspace et des tables
# ════════════════════════════════════════════════════════════════
CREATE_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS solarmboa
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;
"""
# En production, utiliser :
# 'class': 'NetworkTopologyStrategy', 'cameroon-dc1': 3

CREATE_SENSOR_READINGS = """
CREATE TABLE IF NOT EXISTS solarmboa.sensor_readings (
    region          TEXT,
    reading_date    DATE,
    sensor_id       TEXT,
    recorded_at     TIMESTAMP,
    reading_id      UUID,
    installation_id INT,
    solar_output_w  FLOAT,
    battery_pct     FLOAT,
    consumption_w   FLOAT,
    alert_code      TEXT,
    PRIMARY KEY ((region, reading_date), sensor_id, recorded_at, reading_id)
) WITH CLUSTERING ORDER BY (sensor_id ASC, recorded_at DESC)
  AND default_time_to_live = 7776000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  };
"""
# TTL = 90 jours (7 776 000 s) – données IoT temps réel
# TWCS : stratégie de compaction optimale pour séries temporelles

CREATE_SENSOR_LATEST = """
CREATE TABLE IF NOT EXISTS solarmboa.sensor_latest (
    sensor_id       TEXT PRIMARY KEY,
    installation_id INT,
    region          TEXT,
    recorded_at     TIMESTAMP,
    solar_output_w  FLOAT,
    battery_pct     FLOAT,
    consumption_w   FLOAT,
    alert_code      TEXT
);
"""

CREATE_DAILY_SUMMARY = """
CREATE TABLE IF NOT EXISTS solarmboa.daily_region_summary (
    region          TEXT,
    summary_date    DATE,
    total_readings  COUNTER,
    PRIMARY KEY (region, summary_date)
) WITH CLUSTERING ORDER BY (summary_date DESC);
"""


def truncate_tables(session) -> None:
    """
    Vide les tables avant chargement, pour garantir l'idempotence du script
    (même comportement que le flushdb() de Redis ou le drop() de MongoDB) :
    relancer le script ne doit jamais laisser cohabiter d'anciennes données
    (ex: anciennes valeurs de région non normalisées) avec les nouvelles.
    """
    print("\n[1.5/3] Nettoyage des tables existantes (idempotence)...")
    for table in ("sensor_readings", "sensor_latest", "daily_region_summary"):
        session.execute(f"TRUNCATE solarmboa.{table};")
    print("      → sensor_readings, sensor_latest, daily_region_summary vidées")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 2 – Chargement des données
# ════════════════════════════════════════════════════════════════
INSERT_READING = """
INSERT INTO solarmboa.sensor_readings
    (region, reading_date, sensor_id, recorded_at, reading_id,
     installation_id, solar_output_w, battery_pct, consumption_w, alert_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_LATEST = """
INSERT INTO solarmboa.sensor_latest
    (sensor_id, installation_id, region, recorded_at,
     solar_output_w, battery_pct, consumption_w, alert_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
USING TTL 86400
"""

UPDATE_COUNTER = """
UPDATE solarmboa.daily_region_summary
SET total_readings = total_readings + 1
WHERE region = ? AND summary_date = ?
"""


def load_telemetry(session) -> dict:
    print("\n[2/3] Ingestion des données télémétrie...")
    prepared_read = session.prepare(INSERT_READING)
    prepared_read.consistency_level = ConsistencyLevel.ONE
    prepared_latest = session.prepare(INSERT_LATEST)
    prepared_latest.consistency_level = ConsistencyLevel.ONE
    prepared_counter = session.prepare(UPDATE_COUNTER)

    total_rows = 0
    skipped = 0
    errors = 0
    batch_rows = []

    region_date_counts = defaultdict(int)

    t_start = time.perf_counter()

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts = normalize_timestamp(row["timestamp"])
                region = normalize_region(row["region"])
                rdate = ts.date()
                sensor_id = row["sensor_id"].strip()
                inst_id = int(row["installation_id"]) if row["installation_id"] else 0
                solar = float(row["solar_output_w"]) if row["solar_output_w"] else None
                bat = (
                    float(row["battery_level_pct"])
                    if row["battery_level_pct"]
                    else None
                )
                # Clamp battery > 100 (bug firmware connu)
                if bat is not None and bat > 100:
                    bat = 100.0
                cons = float(row["consumption_w"]) if row["consumption_w"] else None
                alert = (row.get("alert_code") or "NONE").upper().strip()

                batch_rows.append(
                    (
                        region,
                        rdate,
                        sensor_id,
                        ts,
                        uuid.uuid4(),
                        inst_id,
                        solar,
                        bat,
                        cons,
                        alert,
                    )
                )
                region_date_counts[(region, rdate)] += 1

                # Flush par batch
                if len(batch_rows) >= BATCH_SIZE:
                    batch = BatchStatement(
                        batch_type=BatchType.UNLOGGED,
                        consistency_level=ConsistencyLevel.ONE,
                    )
                    for r in batch_rows:
                        batch.add(prepared_read, r)
                    session.execute(batch)
                    total_rows += len(batch_rows)
                    batch_rows.clear()

            except (ValueError, KeyError) as e:
                errors += 1
                if errors <= 5:
                    print(f"      ⚠ Erreur ligne {total_rows+errors}: {e}")

    # Flush résidu
    if batch_rows:
        batch = BatchStatement(
            batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.ONE
        )
        for r in batch_rows:
            batch.add(prepared_read, r)
        session.execute(batch)
        total_rows += len(batch_rows)

    elapsed = time.perf_counter() - t_start
    throughput = total_rows / elapsed if elapsed > 0 else 0

    print(f"      → {total_rows:,} lignes insérées en {elapsed:.1f}s")
    print(f"      → Throughput : {throughput:,.0f} rows/sec")
    print(f"      → Erreurs ignorées : {errors}")

    return {"total": total_rows, "elapsed": elapsed, "throughput": throughput}


# ════════════════════════════════════════════════════════════════
# ÉTAPE 3 – Requêtes de validation
# ════════════════════════════════════════════════════════════════
def run_queries(session) -> None:
    print("\n[3/3] Requêtes de validation du schéma...")

    # Requête 1 : dernières lectures d'une région pour une date
    q1 = """
    SELECT sensor_id, recorded_at, solar_output_w, battery_pct, alert_code
    FROM solarmboa.sensor_readings
    WHERE region = 'Littoral' AND reading_date = '2025-01-06'
    LIMIT 5
    """
    rows = session.execute(q1)
    print("\n  Requête 1 – Lectures Littoral 2025-01-06 (limit 5) :")
    for r in rows:
        print(
            f"    {r.sensor_id} | {r.recorded_at} | solar={r.solar_output_w}W | bat={r.battery_pct}% | {r.alert_code}"
        )

    # Requête 2 : dernière mesure d'un capteur précis
    q2 = """
    SELECT sensor_id, recorded_at, solar_output_w, battery_pct
    FROM solarmboa.sensor_readings
    WHERE region = 'Centre' AND reading_date = '2025-06-18'
    LIMIT 3
    """
    rows2 = session.execute(q2)
    print("\n  Requête 2 – Capteurs Centre récents :")
    for r in rows2:
        print(f"    {r.sensor_id} | {r.recorded_at} | {r.solar_output_w}W")

    # Requête 3 : count des partitions chargées
    q3 = "SELECT COUNT(*) FROM solarmboa.sensor_readings WHERE region='Littoral' AND reading_date='2025-01-06'"
    count = session.execute(q3).one()[0]
    print(f"\n  Total lignes dans sensor_readings : {count:,}")


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  SolarMboa – Cassandra Loader (Semaine 5 NoSQL)")
    print("=" * 60)
    print("  Note : premier démarrage Cassandra peut prendre 90s")

    try:
        cluster = Cluster(
            ["localhost"],
            port=9042,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="cameroon-dc1"),
            connect_timeout=30,
        )
        session = cluster.connect()
        print("  Connexion Cassandra OK\n")
    except Exception as e:
        print(f"  ERREUR : Cassandra non accessible – {e}")
        print("  Assurez-vous que Docker est lancé et que Cassandra est prêt :")
        print("  docker compose up -d cassandra && sleep 90")
        exit(1)

    # Créer le keyspace et les tables
    print("[1/3] Création du keyspace et des tables...")
    session.execute(CREATE_KEYSPACE)
    session.set_keyspace("solarmboa")
    session.execute(CREATE_SENSOR_READINGS)
    session.execute(CREATE_SENSOR_LATEST)
    session.execute(CREATE_DAILY_SUMMARY)
    print("      → Tables créées")

    # Nettoyage automatique avant réingestion (idempotence)
    truncate_tables(session)

    stats = load_telemetry(session)
    run_queries(session)

    print(f"\n{'='*60}")
    print(f"  RÉSUMÉ INGESTION CASSANDRA")
    print(f"{'='*60}")
    print(f"  Lignes ingérées : {stats['total']:>10,}")
    print(f"  Durée           : {stats['elapsed']:>10.1f}s")
    print(f"  Throughput      : {stats['throughput']:>10,.0f} rows/sec")
    print(f"  Partition key   : region + reading_date")
    print(f"  Clustering key  : sensor_id + recorded_at (DESC)")
    print(f"\n[OK] Cassandra chargé avec succès.")

    cluster.shutdown()
