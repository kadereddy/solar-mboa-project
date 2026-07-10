# Fiche de Décision CAP – MongoDB

## Profils d'Installations (CRM & Facturation)

### Décision

Utiliser **MongoDB** comme base documentaire pour les profils des 4 800
installations clients (identité, statut, plan tarifaire, localisation).

### Justification par le Théorème CAP

MongoDB est positionné en **CP** (Consistency + Partition Tolerance).

Les données de statut client et de facturation doivent refléter l'état réel
sans ambiguïté : un client suspendu ne doit jamais apparaître comme actif
dans une lecture, même en cas d'incident réseau. Le système privilégie donc
la Consistency, quitte à limiter temporairement la disponibilité en écriture
en cas de partition (le read/write concern peut être configuré à `majority`
pour garantir cette cohérence). Le CRM tolère par ailleurs une brève
indisponibilité, contrairement à un flux temps réel critique.

### Volumétrie Estimée

| Paramètre                 | Valeur                                                                    |
| ------------------------- | ------------------------------------------------------------------------- |
| Documents (installations) | 4 800 (échantillon), 48 000 en cible production                           |
| Structure                 | Semi-structurée, hétérogène par type de client                            |
| Index actifs              | 5 (géospatial 2dsphere, id unique, région+statut, type, tarif)            |
| Répartition régionale     | 7 régions, de 237 (Extrême-Nord) à 1 058 (Littoral) installations actives |

### Pattern d'Accès : **Lecture-intensive** (avec écritures ponctuelles critiques)

- **Lecture** : ~70 % des opérations (consultation profil, agrégations
  CRM, recherche géospatiale pour planification terrain)
- **Écriture** : ~30 % des opérations (changement de statut, nouvelle
  installation, mise à jour tarifaire) — peu fréquentes mais devant être
  strictement cohérentes
- Requêtes dominantes : pipelines d'agrégation (répartition client/statut,
  moyennes par type) et recherche géospatiale (rayon autour d'un point)

**Conclusion** : le besoin d'exactitude sur les données de facturation/statut,
combiné à des écritures peu fréquentes mais sensibles, justifie un choix
**CP** comme MongoDB plutôt qu'un modèle purement disponible.
