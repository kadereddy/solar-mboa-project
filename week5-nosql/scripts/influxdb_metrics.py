# Script d'ingestion InfluxDB
#!/usr/bin/env python3
"""
SolarMboa Technologies – Semaine 5 : InfluxDB
==============================================
Cas d'usage :
  - Métriques production solaire temps réel
  - Requêtes Flux :
      1. Moyenne mobile 15 min par région
      2. Détection de décrochage (baisse > 40% en 5 min)
      3. Ranking des 10 meilleures installations en production journalière

Usage : python3 influxdb_metrics.py
"""

import csv
import os
import time
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ── Connexion ────────────────────────────────────────────────────
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "solarmboa-super-secret-token-2025")
INFLUX_ORG = os.getenv("INFLUX_ORG", "solarmboa-tech")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "solar_metrics")

DATA_FILE = "data/sensors_telemetry.csv"
BATCH_SIZE = 500

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


def normalize_timestamp(raw: str) -> datetime | None:
    raw = raw.strip()
    if raw.isdigit():
        return datetime.utcfromtimestamp(int(raw))
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    return None


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1 – Ingestion des séries temporelles
# ════════════════════════════════════════════════════════════════
def load_solar_metrics(write_api) -> dict:
    print("\n[1/3] Ingestion des métriques solaires dans InfluxDB...")

    total = 0
    skipped = 0
    errors = 0
    batch = []
    t_start = time.perf_counter()

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = normalize_timestamp(row.get("timestamp", ""))
            if ts is None:
                skipped += 1
                continue

            region = normalize_region(row.get("region", ""))
            sensor_id = row.get("sensor_id", "").strip()
            inst_id = row.get("installation_id", "0").strip()

            try:
                solar = float(row["solar_output_w"]) if row["solar_output_w"] else None
                bat = (
                    float(row["battery_level_pct"])
                    if row["battery_level_pct"]
                    else None
                )
                cons = float(row["consumption_w"]) if row["consumption_w"] else None
                alert = (row.get("alert_code") or "NONE").upper().strip()

                # Clamp battery_pct
                if bat is not None and bat > 100:
                    bat = 100.0

                point = (
                    Point("solar_telemetry")
                    .tag("sensor_id", sensor_id)
                    .tag("installation_id", inst_id)
                    .tag("region", region)
                    .tag("alert_code", alert)
                    .time(ts, WritePrecision.S)
                )

                if solar is not None:
                    point.field("solar_output_w", solar)
                if bat is not None:
                    point.field("battery_level_pct", bat)
                if cons is not None:
                    point.field("consumption_w", cons)

                batch.append(point)

                if len(batch) >= BATCH_SIZE:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=batch)
                    total += len(batch)
                    batch.clear()

            except (ValueError, KeyError) as e:
                errors += 1
                if errors <= 3:
                    print(f"      ⚠ Erreur : {e}")

    if batch:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=batch)
        total += len(batch)

    elapsed = time.perf_counter() - t_start
    throughput = total / elapsed if elapsed > 0 else 0

    print(f"      → {total:,} points ingérés en {elapsed:.1f}s")
    print(f"      → Throughput : {throughput:,.0f} points/sec")
    print(f"      → Ignorés (timestamp invalide) : {skipped}")
    print(f"      → Erreurs                      : {errors}")
    return {"total": total, "elapsed": elapsed}


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1.5 – Détection automatique de la plage de dates réelle
# ════════════════════════════════════════════════════════════════
def get_data_time_range(
    query_api, bucket: str, org: str, measurement: str = "solar_telemetry"
):
    """
    Interroge InfluxDB pour trouver la première et la dernière date
    réellement présentes dans les données, au lieu de dépendre d'une
    fenêtre relative à 'maintenant' (ex: -30d) qui peut ne rien retourner
    si les données sont plus anciennes.

    Utilise range(start: 0) = depuis l'epoch Unix, pour être certain de
    couvrir toutes les données peu importe leur ancienneté.

    Retourne (start: datetime, stop: datetime) ou (None, None) si le bucket
    est vide.
    """
    flux_first = f"""
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> filter(fn: (r) => r._field == "solar_output_w")
  |> first()
"""
    flux_last = f"""
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> filter(fn: (r) => r._field == "solar_output_w")
  |> last()
"""
    try:
        first_result = query_api.query(query=flux_first, org=org)
        last_result = query_api.query(query=flux_last, org=org)

        first_time = None
        last_time = None

        for table in first_result:
            for record in table.records:
                t = record.get_time()
                if first_time is None or t < first_time:
                    first_time = t

        for table in last_result:
            for record in table.records:
                t = record.get_time()
                if last_time is None or t > last_time:
                    last_time = t

        if first_time is None or last_time is None:
            return None, None

        # Marge de sécurité de 1 jour de part et d'autre pour ne rater
        # aucun point situé exactement à la limite de la fenêtre
        start = first_time - timedelta(days=1)
        stop = last_time + timedelta(days=1)
        return start, stop

    except Exception as e:
        print(f"  ⚠ Détection de plage impossible : {e}")
        return None, None


def format_flux_time(dt: datetime) -> str:
    """Formate un datetime en littéral de temps Flux (RFC3339 UTC)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 2 – Requêtes Flux analytiques
# ════════════════════════════════════════════════════════════════
def run_flux_queries(query_api, range_clause: str) -> None:
    print("\n[2/3] Requêtes Flux analytiques...")

    # ── Requête 1 : Moyenne mobile 15 min par région ──────────────
    print("\n  REQUÊTE 1 – Moyenne mobile (15 min) production solaire par région")
    flux_q1 = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> {range_clause}
  |> filter(fn: (r) => r._measurement == "solar_telemetry")
  |> filter(fn: (r) => r._field == "solar_output_w")
  |> filter(fn: (r) => r.region != "Inconnu")
  |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
  |> group(columns: ["region"])
  |> mean()
  |> sort(columns: ["_value"], desc: true)
"""
    try:
        result1 = query_api.query(query=flux_q1, org=INFLUX_ORG)
        print(f"  {'Région':<20} {'Moy. Production (W)':>20}")
        print(f"  {'-'*42}")
        for table in result1:
            for record in table.records:
                region = record.values.get("region", "?")
                val = record.get_value() or 0
                print(f"  {region:<20} {val:>20.1f}")
    except Exception as e:
        print(f"  ⚠ Requête 1 : {e}")

    # ── Requête 2 : Détection de décrochage (baisse > 40% en 5 min) ──
    print(
        "\n  REQUÊTE 2 – Détection décrochage production (baisse > 40% sur fenêtre 5 min)"
    )
    flux_q2 = f"""
data = from(bucket: "{INFLUX_BUCKET}")
  |> {range_clause}
  |> filter(fn: (r) => r._measurement == "solar_telemetry")
  |> filter(fn: (r) => r._field == "solar_output_w")
  |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)

data
  |> derivative(unit: 5m, nonNegative: false)
  |> filter(fn: (r) => r._value < -0.40)
  |> group(columns: ["sensor_id", "region"])
  |> count()
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 10)
"""
    try:
        result2 = query_api.query(query=flux_q2, org=INFLUX_ORG)
        count_total = sum(
            record.get_value() for table in result2 for record in table.records
        )
        print(
            f"  → {count_total} événements de décrochage détectés (top 10 capteurs affichés)"
        )
        for table in result2:
            for record in table.records:
                sid = record.values.get("sensor_id", "?")
                region = record.values.get("region", "?")
                count = record.get_value()
                print(f"    {sid:<20} [{region}] : {count} décrochages")
    except Exception as e:
        print(f"  ⚠ Requête 2 : {e}")

    # ── Requête 3 : Top 10 installations en production journalière ──
    print("\n  REQUÊTE 3 – Top 10 installations en production journalière totale")
    flux_q3 = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> {range_clause}
  |> filter(fn: (r) => r._measurement == "solar_telemetry")
  |> filter(fn: (r) => r._field == "solar_output_w")
  |> group(columns: ["installation_id", "region"])
  |> integral(unit: 1h)
  |> map(fn: (r) => ({{r with _value: r._value / 1000.0}}))
  |> sort(columns: ["_value"], desc: true)
  |> limit(n: 10)
"""
    try:
        result3 = query_api.query(query=flux_q3, org=INFLUX_ORG)
        print(
            f"  {'Rank':<5} {'Installation ID':<18} {'Région':<14} {'Production (kWh)':>18}"
        )
        print(f"  {'-'*58}")
        rank = 1
        for table in result3:
            for record in table.records:
                inst_id = record.values.get("installation_id", "?")
                region = record.values.get("region", "?")
                kwh = record.get_value() or 0
                print(f"  {rank:<5} {inst_id:<18} {region:<14} {kwh:>18.2f}")
                rank += 1
    except Exception as e:
        print(f"  ⚠ Requête 3 : {e}")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 3 – Stats bucket
# ════════════════════════════════════════════════════════════════
def print_bucket_stats(query_api, range_clause: str) -> None:
    print("\n[3/3] Statistiques du bucket solar_metrics :")
    flux_count = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> {range_clause}
  |> filter(fn: (r) => r._measurement == "solar_telemetry")
  |> filter(fn: (r) => r._field == "solar_output_w")
  |> count()
  |> sum()
"""
    try:
        result = query_api.query(query=flux_count, org=INFLUX_ORG)
        total = sum(record.get_value() for table in result for record in table.records)
        print(f"  Total points solar_output_w : {total:,}")
    except Exception as e:
        print(f"  ⚠ Stats : {e}")


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  SolarMboa – InfluxDB Loader (Semaine 5 NoSQL)")
    print("=" * 60)

    try:
        client = InfluxDBClient(
            url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=180_000
        )
        health = client.health()
        print(f"  Connexion InfluxDB OK – status: {health.status}\n")
    except Exception as e:
        print(f"  ERREUR : InfluxDB non accessible – {e}")
        print("  Assurez-vous que Docker est lancé : docker compose up -d influxdb")
        exit(1)

    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()

    stats = load_solar_metrics(write_api)

    # ── Détection automatique de la plage réelle des données ──────
    print("\n[Info] Détection automatique de la plage de dates des données...")
    range_start, range_stop = get_data_time_range(query_api, INFLUX_BUCKET, INFLUX_ORG)

    if range_start is None:
        print("  ⚠ Aucune donnée trouvée / détection impossible, repli sur -30d")
        range_clause = "range(start: -30d)"
        range_label = "30 derniers jours (repli)"
    else:
        start_str = format_flux_time(range_start)
        stop_str = format_flux_time(range_stop)
        print(f"  → Plage détectée : {start_str} → {stop_str}")
        range_clause = f"range(start: {start_str}, stop: {stop_str})"
        range_label = f"{start_str} → {stop_str}"

    run_flux_queries(query_api, range_clause)
    print_bucket_stats(query_api, range_clause)

    print(f"\n{'='*60}")
    print(f"  RÉSUMÉ INGESTION INFLUXDB")
    print(f"{'='*60}")
    print(f"  Points ingérés  : {stats['total']:>10,}")
    print(f"  Durée           : {stats['elapsed']:>10.1f}s")
    print(f"  Bucket          : {INFLUX_BUCKET}")
    print(f"  Plage analysée  : {range_label}")
    print(f"  UI InfluxDB     : http://localhost:8086")
    print(f"\n[OK] InfluxDB chargé avec succès.")

    client.close()
