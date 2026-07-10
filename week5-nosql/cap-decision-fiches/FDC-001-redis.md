# Fiche de Décision CAP – Redis

## Cache des Sessions IoT (Compteurs Actifs)

### Décision

Utiliser **Redis** comme cache en mémoire pour l'état courant des 5 000
compteurs IoT les plus actifs (facturation, activation/désactivation).

### Justification par le Théorème CAP

Redis est positionné en **AP** (Availability + Partition Tolerance).

En cas de partition réseau, le système privilégie la disponibilité : mieux
vaut répondre avec un état légèrement périmé (quelques secondes) que de
bloquer une opération terrain (activation, vérification de statut). Une
incohérence transitoire sur l'affichage d'un compteur est acceptable ; une
indisponibilité du cache lors d'une opération de facturation ne l'est pas.
La Consistency est donc volontairement sacrifiée au profit de l'Availability
et de la Partition Tolerance.

### Volumétrie Estimée

| Paramètre                   | Valeur                                   |
| --------------------------- | ---------------------------------------- |
| Compteurs en cache (actifs) | 5 000 (sur 48 000 installations totales) |
| Taille moyenne par entrée   | ~0,58 Ko (mesuré : 2,90 Mo / 5 000 clés) |
| Volume total estimé         | ~3 Mo pour 5 000 sessions                |
| TTL                         | 86 400 s (24h, cycle de facturation)     |
| Commandes en file d'attente | 50 (activation/désactivation)            |

### Pattern d'Accès : **Lecture-intensive**

- **Lecture** : ~95 % des opérations (vérification d'état avant facturation/action)
- **Écriture** : ~5 % des opérations (mise à jour périodique de l'état capteur)
- Accès par clé directe (`sensor:state:{sensor_id}`), aucun scan requis
- Gain mesuré vs lecture CSV brute : **6,4x** plus rapide (76 ms vs 484 ms
  sur 1 000 lectures en pipeline)

**Conclusion** : le profil très lecture-intensive, la tolérance à une
consistance faible, et l'exigence de disponibilité continue justifient un
cache **AP** comme Redis plutôt qu'une base transactionnelle.
