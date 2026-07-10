# Fiche de Décision CAP – Cassandra

## Télémétrie IoT Haute Fréquence

### Décision

Utiliser **Cassandra** comme base distribuée pour l'ingestion continue des
mesures brutes de télémétrie (production solaire, batterie, consommation,
alertes) des 48 000 installations IoT.

### Justification par le Théorème CAP

Cassandra est positionné en **AP** (Availability + Partition Tolerance),
avec `ConsistencyLevel.ONE` utilisé à l'écriture.

L'ingestion IoT ne doit jamais être bloquée, même en cas de nœud
indisponible ou de partition réseau : une écriture confirmée par une seule
réplique suffit pour maximiser le débit. Une légère incohérence temporaire
entre répliques est acceptable pour des mesures brutes à haute fréquence ;
un blocage d'écriture, en revanche, entraînerait une perte de données
irréversible à la source. La disponibilité d'écriture prime donc
explicitement sur la cohérence immédiate entre répliques.

### Volumétrie Estimée

| Paramètre                            | Valeur                                             |
| ------------------------------------ | -------------------------------------------------- |
| Lignes ingérées (échantillon projet) | 120 960                                            |
| Cible production                     | ~12 000 000 événements/jour                        |
| Débit mesuré                         | 1 592 lignes/sec (nœud unique, conteneur Docker)   |
| Rétention (TTL)                      | 90 jours (7 776 000 s)                             |
| Clé de partition                     | région + date (distribue naturellement l'écriture) |

### Pattern d'Accès : **Écriture-intensive**

- **Écriture** : dominante et continue, en batchs de 50 lignes
  (`BatchStatement UNLOGGED`), flux constant depuis les capteurs terrain
- **Lecture** : minoritaire, toujours ciblée par partition (région + date),
  jamais de scan global (les `COUNT(*)` non bornés provoquent des échecs
  serveur — anti-pattern identifié et évité)
- Compaction `TimeWindowCompactionStrategy` optimisée pour ce profil
  d'écriture temporelle avec expiration automatique (TTL)

**Conclusion** : le volume d'écriture massif et continu, distribué
naturellement par région et date, justifie un choix **AP** comme Cassandra
plutôt qu'une base priorisant la cohérence au détriment du débit.
