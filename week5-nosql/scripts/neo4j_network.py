# Script d'ingestion Neo4j
#!/usr/bin/env python3
"""
SolarMboa Technologies – Semaine 5 : Neo4j
===========================================
Cas d'usage :
  - Cartographie réseau de distribution (Distributeur → Technicien → Installation)
  - Détection d'anomalies : techniciens fantômes, circuits commission suspects
  - 3 requêtes Cypher :
      1. Chemin le plus court entre deux régions
      2. Techniciens avec taux d'anomalie > seuil
      3. Sous-graphe de fraude potentielle

Usage : python3 neo4j_network.py
"""

import csv
import os
import random
from datetime import datetime, date, timedelta
from neo4j import GraphDatabase

# ── Connexion ────────────────────────────────────────────────────
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "solarmboa2025")
DATA_DIR = "data"


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1 – Contraintes et Index
# ════════════════════════════════════════════════════════════════
CONSTRAINTS = [
    "CREATE CONSTRAINT dist_id   IF NOT EXISTS FOR (d:Distributor)  REQUIRE d.id IS UNIQUE",
    "CREATE CONSTRAINT tech_id   IF NOT EXISTS FOR (t:Technician)   REQUIRE t.id IS UNIQUE",
    "CREATE CONSTRAINT inst_id   IF NOT EXISTS FOR (i:Installation) REQUIRE i.id IS UNIQUE",
    "CREATE CONSTRAINT region_id IF NOT EXISTS FOR (r:Region)       REQUIRE r.name IS UNIQUE",
]

INDEXES = [
    "CREATE INDEX dist_region  IF NOT EXISTS FOR (d:Distributor)  ON (d.region)",
    "CREATE INDEX tech_region  IF NOT EXISTS FOR (t:Technician)   ON (t.region)",
    "CREATE INDEX inst_status  IF NOT EXISTS FOR (i:Installation) ON (i.status)",
    "CREATE INDEX inst_region  IF NOT EXISTS FOR (i:Installation) ON (i.region)",
]


def clear_graph(driver) -> None:
    """
    Supprime tous les nœuds et relations avant chargement, pour garantir
    l'idempotence du script (même comportement que le flushdb() de Redis
    ou le drop() de MongoDB) : relancer le script ne doit jamais laisser
    cohabiter d'anciennes données (ex: anciennes valeurs de région non
    normalisées) avec les nouvelles.

    Les contraintes et index (créés séparément avec IF NOT EXISTS) ne sont
    pas affectés par ce nettoyage : seuls les nœuds/relations sont supprimés.

    Suppression par lots (LIMIT) pour éviter un unique gros transaction sur
    des graphes volumineux, qui pourrait saturer la mémoire.
    """
    print("\n[1.5/5] Nettoyage du graphe existant (idempotence)...")
    with driver.session() as s:
        deleted_total = 0
        while True:
            result = s.run("""
                MATCH (n)
                WITH n LIMIT 5000
                DETACH DELETE n
                RETURN count(n) AS deleted
            """)
            deleted = result.single()["deleted"]
            deleted_total += deleted
            if deleted == 0:
                break
    print(f"      → {deleted_total:,} nœuds (et leurs relations) supprimés")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 2 – Chargement des nœuds
# ════════════════════════════════════════════════════════════════
def load_nodes(driver) -> None:
    print("\n[2/5] Chargement des nœuds...")

    # ── Régions ──
    regions = [
        "Littoral",
        "Centre",
        "Ouest",
        "Nord-Ouest",
        "Adamaoua",
        "Est",
        "Extrême-Nord",
    ]
    with driver.session() as s:
        for r in regions:
            s.run("MERGE (:Region {name: $name})", name=r)
    print(f"      → {len(regions)} nœuds Région créés")

    # ── Distributeurs ──
    dist_count = 0
    with open(
        f"{DATA_DIR}/network_nodes_distributors.csv", newline="", encoding="utf-8"
    ) as f:
        reader = csv.DictReader(f)
        with driver.session() as s:
            for row in reader:
                s.run(
                    """
                    MERGE (d:Distributor {id: $id})
                    SET d.name   = $name,
                        d.region = $region,
                        d.since  = $since,
                        d.type   = 'distributor'
                """,
                    id=row["id"],
                    name=row["name"],
                    region=row["region"],
                    since=row["since"],
                )
                dist_count += 1
    print(f"      → {dist_count} nœuds Distributeur créés")

    # ── Techniciens ──
    tech_count = 0
    with open(
        f"{DATA_DIR}/network_nodes_technicians.csv", newline="", encoding="utf-8"
    ) as f:
        reader = csv.DictReader(f)
        with driver.session() as s:
            for row in reader:
                # Simulation : anomaly_score entre 0 et 1
                anomaly_score = round(random.uniform(0, 0.35), 3)
                # 5% des techniciens sont marqués "fantômes" (aucune intervention récente)
                is_ghost = random.random() < 0.05
                s.run(
                    """
                    MERGE (t:Technician {id: $id})
                    SET t.name          = $name,
                        t.region        = $region,
                        t.phone         = $phone,
                        t.certified     = $cert,
                        t.anomaly_score = $score,
                        t.is_ghost      = $ghost,
                        t.type          = 'technician'
                """,
                    id=row["id"],
                    name=row["name"],
                    region=row["region"],
                    phone=row["phone"],
                    cert=(row["certified"].lower() == "true"),
                    score=anomaly_score,
                    ghost=is_ghost,
                )
                tech_count += 1
    print(f"      → {tech_count} nœuds Technicien créés")

    # ── Installations (subset léger : id + region + status) ──
    import json

    with open(f"{DATA_DIR}/installations.json", encoding="utf-8") as f:
        installations = json.load(f)

    batch_size = 500
    inst_count = 0
    with driver.session() as s:
        batch = []
        for inst in installations:
            batch.append(
                {
                    "id": inst["installation_id"],
                    "client_name": inst["client_name"],
                    "client_type": inst["client_type"],
                    "region": inst.get("region", "Inconnu"),
                    "status": inst.get("status", "active"),
                    "tariff": inst.get("tariff_plan", "Basic"),
                }
            )
            if len(batch) >= batch_size:
                s.run(
                    """
                    UNWIND $rows AS row
                    MERGE (i:Installation {id: row.id})
                    SET i.client_name = row.client_name,
                        i.client_type = row.client_type,
                        i.region      = row.region,
                        i.status      = row.status,
                        i.tariff      = row.tariff
                """,
                    rows=batch,
                )
                inst_count += len(batch)
                batch.clear()
        if batch:
            s.run(
                """
                UNWIND $rows AS row
                MERGE (i:Installation {id: row.id})
                SET i.client_name = row.client_name,
                    i.client_type = row.client_type,
                    i.region      = row.region,
                    i.status      = row.status,
                    i.tariff      = row.tariff
            """,
                rows=batch,
            )
            inst_count += len(batch)
    print(f"      → {inst_count} nœuds Installation créés")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 3 – Chargement des relations
# ════════════════════════════════════════════════════════════════
def load_relationships(driver) -> None:
    print("\n[3/5] Chargement des relations depuis network_graph.csv...")

    sold_count = 0
    employs_count = 0
    maintains_count = 0
    skip_count = 0

    def parse_date_rel(raw: str) -> str:
        if not raw:
            return ""
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return raw.strip()

    with open(f"{DATA_DIR}/network_graph.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Traitement par type de relation
    batch_sold = []
    batch_employs = []
    batch_maintains = []

    for row in rows:
        rel = row["relation"].strip().upper()
        src_type = row["source_type"].strip()
        tgt_type = row["target_type"].strip()
        rel_date = parse_date_rel(row.get("date", ""))

        if rel == "SOLD" and src_type == "distributor" and tgt_type == "installation":
            batch_sold.append(
                {
                    "dist_id": row["source_id"],
                    "inst_id": int(row["target_id"]),
                    "date": rel_date,
                }
            )
        elif (
            rel == "EMPLOYS" and src_type == "distributor" and tgt_type == "technician"
        ):
            batch_employs.append(
                {
                    "dist_id": row["source_id"],
                    "tech_id": row["target_id"],
                    "date": rel_date,
                }
            )
        elif (
            rel == "MAINTAINS"
            and src_type == "technician"
            and tgt_type == "installation"
        ):
            # Simulation du score de commission (pour détection fraude)
            commission = round(random.uniform(500, 15000), 2)
            batch_maintains.append(
                {
                    "tech_id": row["source_id"],
                    "inst_id": int(row["target_id"]),
                    "date": rel_date,
                    "commission": commission,
                    "weight": float(row.get("weight") or 1.0),
                }
            )
        else:
            skip_count += 1

    BATCH = 500
    with driver.session() as s:
        # SOLD
        for i in range(0, len(batch_sold), BATCH):
            s.run(
                """
                UNWIND $rows AS row
                MATCH (d:Distributor {id: row.dist_id})
                MATCH (i:Installation {id: row.inst_id})
                MERGE (d)-[r:SOLD]->(i)
                SET r.date = row.date
            """,
                rows=batch_sold[i : i + BATCH],
            )
        sold_count = len(batch_sold)

        # EMPLOYS
        for i in range(0, len(batch_employs), BATCH):
            s.run(
                """
                UNWIND $rows AS row
                MATCH (d:Distributor {id: row.dist_id})
                MATCH (t:Technician {id: row.tech_id})
                MERGE (d)-[r:EMPLOYS]->(t)
                SET r.since = row.date
            """,
                rows=batch_employs[i : i + BATCH],
            )
        employs_count = len(batch_employs)

        # MAINTAINS
        for i in range(0, len(batch_maintains), BATCH):
            s.run(
                """
                UNWIND $rows AS row
                MATCH (t:Technician {id: row.tech_id})
                MATCH (i:Installation {id: row.inst_id})
                MERGE (t)-[r:MAINTAINS]->(i)
                SET r.date       = row.date,
                    r.commission = row.commission,
                    r.weight     = row.weight
            """,
                rows=batch_maintains[i : i + BATCH],
            )
        maintains_count = len(batch_maintains)

        # Relier Distributeurs et Techniciens à leurs Régions
        s.run("""
            MATCH (d:Distributor)
            MATCH (r:Region {name: d.region})
            MERGE (d)-[:OPERATES_IN]->(r)
        """)
        s.run("""
            MATCH (t:Technician)
            MATCH (r:Region {name: t.region})
            MERGE (t)-[:WORKS_IN]->(r)
        """)

    print(f"      → SOLD       : {sold_count:,} relations")
    print(f"      → EMPLOYS    : {employs_count:,} relations")
    print(f"      → MAINTAINS  : {maintains_count:,} relations")
    print(f"      → Ignorées   : {skip_count}")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 4 – 3 requêtes Cypher de détection d'anomalies
# ════════════════════════════════════════════════════════════════
def run_cypher_queries(driver) -> None:
    print("\n[4/5] Requêtes Cypher analytiques...")

    with driver.session() as s:

        # ── Requête 1 : Chemin le plus court entre deux régions ──
        print("\n  REQUÊTE 1 – Chemin le plus court Littoral → Adamaoua")
        q1 = """
        MATCH path = shortestPath(
            (r1:Region {name: 'Littoral'})-[*]-(r2:Region {name: 'Adamaoua'})
        )
        RETURN [node IN nodes(path) | coalesce(node.name, node.id, toString(node.id))] AS chemin,
               length(path) AS longueur
        LIMIT 1
        """
        result = s.run(q1)
        for r in result:
            print(f"    Chemin : {' → '.join(str(x) for x in r['chemin'])}")
            print(f"    Longueur : {r['longueur']} sauts")

        # ── Requête 2 : Techniciens avec taux d'anomalie > seuil ──
        ANOMALY_THRESHOLD = 0.25
        print(
            f"\n  REQUÊTE 2 – Techniciens suspect (anomaly_score > {ANOMALY_THRESHOLD})"
        )
        q2 = """
        MATCH (t:Technician)-[:MAINTAINS]->(i:Installation)
        WHERE t.anomaly_score > $threshold
        WITH t, count(i) AS nb_installations
        RETURN t.id          AS technician_id,
               t.name        AS nom,
               t.region      AS region,
               t.certified   AS certifie,
               t.anomaly_score AS score_anomalie,
               nb_installations
        ORDER BY t.anomaly_score DESC
        LIMIT 10
        """
        result2 = s.run(q2, threshold=ANOMALY_THRESHOLD)
        print(
            f"  {'ID':<12} {'Nom':<25} {'Région':<12} {'Certifié':>8} {'Score':>7} {'Nb Install':>10}"
        )
        print(f"  {'-'*75}")
        rows2 = list(result2)
        for r in rows2:
            cert = "✓" if r["certifie"] else "✗"
            print(
                f"  {r['technician_id']:<12} {r['nom']:<25} {r['region']:<12} {cert:>8} {r['score_anomalie']:>7.3f} {r['nb_installations']:>10}"
            )
        if not rows2:
            print("  Aucun technicien suspect trouvé (scores tous en dessous du seuil)")

        # ── Requête 3 : Sous-graphe de fraude potentielle ──
        # Techniciens fantômes (is_ghost=true) qui maintiennent quand même des installations
        print("\n  REQUÊTE 3 – Sous-graphe fraude : techniciens fantômes actifs")
        q3 = """
        MATCH (d:Distributor)-[:EMPLOYS]->(t:Technician {is_ghost: true})-[m:MAINTAINS]->(i:Installation)
        RETURN d.id         AS distributeur,
               d.name       AS nom_distributeur,
               t.id         AS technicien,
               t.name       AS nom_tech,
               count(i)     AS installations_maintenues,
               sum(m.commission) AS commissions_totales_xaf
        ORDER BY commissions_totales_xaf DESC
        LIMIT 10
        """
        result3 = s.run(q3)
        print(
            f"  {'Distributeur':<12} {'Technicien':<12} {'Nb Inst.':>8} {'Commissions (XAF)':>20}"
        )
        print(f"  {'-'*60}")
        rows3 = list(result3)
        for r in rows3:
            print(
                f"  {r['distributeur']:<12} {r['technicien']:<12} {r['installations_maintenues']:>8} {r['commissions_totales_xaf']:>20,.0f}"
            )
        if not rows3:
            print("  Aucun technicien fantôme actif détecté")


# ════════════════════════════════════════════════════════════════
# ÉTAPE 5 – Stats du graphe
# ════════════════════════════════════════════════════════════════
def print_graph_stats(driver) -> None:
    print("\n[5/5] Statistiques du graphe :")
    with driver.session() as s:
        node_counts = s.run("""
            MATCH (n)
            RETURN labels(n)[0] AS label, count(n) AS count
            ORDER BY count DESC
        """)
        rel_counts = s.run("""
            MATCH ()-[r]->()
            RETURN type(r) AS type, count(r) AS count
            ORDER BY count DESC
        """)
        print("\n  Nœuds :")
        for r in node_counts:
            print(f"    {r['label']:<15} : {r['count']:>6,}")
        print("\n  Relations :")
        for r in rel_counts:
            print(f"    {r['type']:<15} : {r['count']:>6,}")


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  SolarMboa – Neo4j Loader (Semaine 5 NoSQL)")
    print("=" * 60)

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        driver.verify_connectivity()
        print("  Connexion Neo4j OK\n")
    except Exception as e:
        print(f"  ERREUR : Neo4j non accessible – {e}")
        print("  Assurez-vous que Docker est lancé : docker compose up -d neo4j")
        exit(1)

    print("[1/5] Création des contraintes et index...")
    with driver.session() as s:
        for q in CONSTRAINTS:
            s.run(q)
        for q in INDEXES:
            s.run(q)
    print("      → Contraintes et index OK")

    # Nettoyage automatique avant réingestion (idempotence)
    clear_graph(driver)

    load_nodes(driver)
    load_relationships(driver)
    run_cypher_queries(driver)
    print_graph_stats(driver)

    print("\n[OK] Neo4j chargé avec succès.")
    print("     Interface browser : http://localhost:7474")
    driver.close()
