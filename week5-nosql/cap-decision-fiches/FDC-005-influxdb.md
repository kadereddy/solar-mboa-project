# Fiche de Décision CAP – InfluxDB

## Métriques Analytiques de Production Solaire

### Décision

Utiliser **InfluxDB** comme base de séries temporelles pour les analyses
dérivées de production solaire : moyennes mobiles par région, détection de
décrochages, classement des installations les plus performantes.

### Justification par le Théorème CAP

InfluxDB est positionné en **AP** (Availability + Partition Tolerance).

L'ingestion de métriques ne doit pas être bloquée par des calculs
analytiques en cours, et un léger délai entre écriture et disponibilité en
lecture est acceptable pour du reporting (contrairement à une opération
transactionnelle). En instance OSS mono-nœud (utilisée ici), la tolérance de
partition réelle nécessiterait une édition Cluster/Enterprise ; le choix AP
reflète néanmoins la priorité fonctionnelle du système : ingestion continue
et requêtage flexible plutôt que cohérence stricte immédiate.

### Volumétrie Estimée

| Paramètre                           | Valeur                                                                                                                                             |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Points ingérés (échantillon projet) | 120 960                                                                                                                                            |
| Débit mesuré                        | 1 052 – 2 681 points/sec (variable selon charge machine)                                                                                           |
| Champs par point                    | solar_output_w, battery_level_pct, consumption_w                                                                                                   |
| Tags par point                      | sensor_id, installation_id, region, alert_code                                                                                                     |
| Rétention                           | Configurée en infini pour permettre l'analyse rétroactive d'historique 2025 (une fenêtre glissante 30-90j serait retenue en production temps réel) |

### Pattern d'Accès : **Écriture-intensive en ingestion, lecture-intensive en analyse**

- **Écriture** : par batch de 500 points, flux régulier depuis les capteurs
- **Lecture** : requêtes Flux complexes et coûteuses en calcul (fenêtres
  glissantes, dérivées, intégrales sur plusieurs mois de données),
  typiquement asynchrones (rapports, tableaux de bord)
- Complémentaire de Cassandra : InfluxDB porte les fonctions d'agrégation
  temporelle natives que Cassandra n'exprime pas nativement

**Conclusion** : le besoin de fonctions d'agrégation temporelle natives
(moyenne mobile, dérivée, intégrale) sur un flux d'ingestion continu
justifie un choix **AP** comme InfluxDB, en complément de Cassandra pour
la télémétrie brute à très haut débit.
