# Fiche de Décision CAP – Neo4j

## Réseau de Distribution & Détection de Fraude

### Décision

Utiliser **Neo4j** comme base orientée graphe pour modéliser le réseau
Distributeur → Technicien → Installation et détecter des schémas de fraude
(techniciens fantômes, commissions anormales, connectivité inter-régions).

### Justification par le Théorème CAP

Neo4j est positionné en **CP** (Consistency + Partition Tolerance en
configuration cluster).

Les relations de commission et de maintenance doivent être exactes : une
incohérence fausserait directement une enquête de fraude ou un calcul de
commission. Le système privilégie donc la Consistency. En instance unique
(Community Edition, utilisée ici), la tolérance de partition réelle est
limitée ; une configuration Enterprise (Causal Cluster) serait nécessaire
en production pour combiner CP avec une haute disponibilité. Les analyses
de réseau étant typiquement asynchrones (batch), une brève indisponibilité
reste acceptable.

### Volumétrie Estimée

| Paramètre                                      | Valeur                                                |
| ---------------------------------------------- | ----------------------------------------------------- |
| Nœuds Région                                   | 7                                                     |
| Nœuds Distributeur / Technicien / Installation | Variable selon jeu de données (installations : 4 800) |
| Relations                                      | SOLD, EMPLOYS, MAINTAINS, OPERATES_IN, WORKS_IN       |
| Techniciens marqués "fantômes" (simulation)    | ~5 % de l'effectif                                    |

### Pattern d'Accès : **Lecture-intensive (analytique)**

- **Écriture** : ponctuelle, en batch lors du chargement initial du réseau
  (nœuds puis relations), peu fréquente en régime établi
- **Lecture** : dominante — requêtes de parcours multi-sauts (`shortestPath`
  entre régions), détection de motifs de fraude (sous-graphes), calcul de
  scores d'anomalie par technicien
- Les parcours de graphe profonds (plusieurs sauts) seraient coûteux à
  exprimer en SQL relationnel ; Cypher les exprime nativement en une requête

**Conclusion** : le besoin d'exactitude sur les relations de fraude/commission,
combiné à un usage dominé par des requêtes analytiques de parcours multi-sauts,
justifie un choix **CP** comme Neo4j plutôt qu'un modèle relationnel ou
purement disponible.
