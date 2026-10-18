# Benchmark des solutions CDC

> **Périmètre et fraîcheur.** Cette page synthétise un benchmark de cadrage pour comparer des approches CDC, notamment dans un contexte Oracle et PostgreSQL. Les connecteurs, versions, licences, restrictions des services managés et tarifs évoluent. Les points ci-dessous ne constituent pas une vérification des offres commerciales en vigueur au 18 octobre 2026. Vérifier la documentation et les contrats des fournisseurs avant un choix de production ou d'achat.

## 1. Grandes approches CDC

| Approche | Principe | Impact sur la source | Capture des suppressions | Latence typique |
| --- | --- | --- | --- | --- |
| Log-based | Lire le journal transactionnel (par exemple WAL PostgreSQL ou redo logs Oracle) | Généralement faible, hors charge de lecture et configuration de réplication | Oui, si le connecteur et la configuration prennent en charge l'événement | Secondes ou moins, selon charge et configuration |
| Trigger-based | Déclencher l'écriture d'événements dans une table d'audit lors des changements | Peut être significatif: exécution supplémentaire et écritures synchrones | Oui, si les triggers couvrent les suppressions | Faible à quelques secondes selon architecture |
| Query-based | Interroger périodiquement les tables, souvent avec un curseur temporel tel que `updated_at` | Charge de requête périodique | Pas de façon fiable sans indicateur de suppression ou mécanisme complémentaire | Dépend de la fréquence de polling, souvent secondes à minutes |

Le log-based est généralement le meilleur point de départ lorsqu'il faut une capture fidèle des changements avec faible latence et peu d'impact applicatif. Il ne rend pas automatiquement une solution « sérieuse » ou adaptée à la production: l'exploitation, la reprise, la rétention des journaux, les limites du connecteur et les exigences de cohérence restent à concevoir. Les approches trigger-based et query-based peuvent convenir à des contraintes différentes.

## 2. Comparatif des solutions

| Solution | Type / licence (à confirmer) | Oracle | PostgreSQL | Autres sources | Cibles / intégrations | Forces | Limites |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Debezium** | Open source, Apache 2.0 pour le projet; support commercial distinct selon distribution | Connecteur Oracle; LogMiner ou XStream selon configuration, version et prérequis/licences Oracle | Connecteur PostgreSQL utilisant notamment `pgoutput` | Plusieurs connecteurs; disponibilité et maturité varient par source | Kafka Connect; Debezium Server vers des destinations prises en charge; intégrations de traitement de flux | Écosystème ouvert, événements cohérents entre connecteurs, snapshots et transformations selon connecteur | Kafka/Connect ou le runtime choisi restent à exploiter; limites et fonctions varient par base et version |
| **Oracle GoldenGate** | Commercial | Intégration Oracle native et approfondie | Des options de réplication existent selon produit/version et scénario | Bases et plateformes prises en charge par l'offre | Nombreuses cibles selon produit/version | Fonctionnalités Oracle avancées, réplication et support éditeur | Coût, complexité et dépendance à l'écosystème Oracle |
| **Qlik Replicate** | Commercial | Connectivité disponible selon versions et environnements | Connectivité disponible selon versions et environnements | Large portefeuille de sources, à vérifier | Entrepôts, plateformes cloud et autres cibles prises en charge | Interface et parcours d'intégration orientés réplication | Licence commerciale; périmètre et coûts dépendent de l'offre |
| **Striim** | Commercial | Connecteurs et capacités dépendants de l'offre | Connecteurs et capacités dépendants de l'offre | Plusieurs sources | Cloud, entrepôts et plateformes de flux selon l'offre | Capture et traitement de flux dans une même plateforme | Coût et choix de plateforme; vérifier les connecteurs et SLA requis |
| **Fivetran HVR** | Commercial | Capacités de réplication à vérifier selon version et architecture | Capacités de réplication à vérifier selon version et architecture | Plusieurs sources | Entrepôts et lacs de données selon l'offre | Réplication et fonctions de validation disponibles selon produit | Coût et intégration à l'écosystème Fivetran |
| **Confluent Oracle CDC Connector** | Connecteur commercial Confluent | Connecteur Oracle CDC; mécanisme et prérequis à confirmer dans la version sous licence | Sans objet pour le connecteur Oracle; évaluer Debezium pour PostgreSQL | Principalement orienté Oracle pour ce connecteur | Kafka / Confluent Platform | Support éditeur dans l'écosystème Confluent | Ciblé Oracle et dépendant de l'écosystème/licence Confluent |
| **AWS Database Migration Service (DMS)** | Service managé AWS | Pris en charge selon source, version et mode | Pris en charge selon source, version et mode | Plusieurs bases et sources | Principalement services AWS, avec destinations supportées par DMS | Intégration managée dans AWS; moins d'infrastructure à opérer soi-même | Latence et fonctions varient; vérifier LOB, DDL, limites et régions |
| **Google Cloud Datastream** | Service managé Google Cloud | Sources et versions explicitement prises en charge par Datastream | Sources et versions explicitement prises en charge par Datastream | Périmètre dépendant des sources supportées par le service | Services Google Cloud et destinations supportées | Service géré, sans cluster Kafka Connect à exploiter soi-même | Dépendance GCP; vérifier les sources, régions, limites et destinations actuelles |
| **Flink CDC** | Open source; coût d'exploitation Flink | Connecteurs disponibles selon version et architecture | Connecteurs disponibles selon version et architecture | Plusieurs bases selon la distribution | Apache Flink | Capture intégrée à des traitements de flux avec état | Nécessite Flink et ses compétences opérationnelles; compatibilité versionnée |
| **Airbyte / Estuary** | Open source, commercial ou SaaS selon produit | Prise en charge et mode CDC à vérifier par connecteur | Prise en charge et mode CDC à vérifier par connecteur | Large catalogue, maturité variable | Entrepôts, lacs et autres destinations | Mise en place de pipelines ELT et nombreux connecteurs | Pas toujours adapté aux exigences de streaming basse latence; fonctions variables |
| **OpenLogReplicator** | Projet open source | Lecture directe des redo logs Oracle | Non applicable | Principalement Oracle | Kafka et intégrations selon l'environnement | Option spécialisée pour la capture Oracle | Périmètre plus ciblé; intégration, support et exploitation à évaluer séparément |
| **`pgoutput`, `wal2json`, `pglogical` seuls** | Primitives/extensions open source | Non applicable | Oui, avec les différences propres à chaque mécanisme | Non applicable | À construire: consumer, stockage d'offsets, reprise et routage | Briques PostgreSQL; `pgoutput` est le plugin logique natif | Il faut construire et opérer le reste du pipeline |
| **Maxwell, Canal** | Open source | Non | Non | Principalement MySQL | Kafka et autres intégrations selon projet | Solutions spécialisées dans des scénarios MySQL | Hors périmètre Oracle/PostgreSQL; vérifier la maintenance et la compatibilité actuelles |

Les libellés « pris en charge » ne remplacent pas une matrice de compatibilité: confirmer les versions de base, éditions, modes de déploiement, types de données, fonctions CDC, destinations, restrictions de réseau et niveaux de support.

## 3. Pourquoi considérer Debezium pour Oracle et PostgreSQL

1. **Une famille de connecteurs pour plusieurs bases.** Debezium propose des connecteurs source dédiés, avec une enveloppe d'événement et un modèle d'exploitation cohérents. Les champs `before`, `after`, `op` et `source` sont courants, mais leurs détails et disponibilités dépendent du connecteur et de la configuration.
2. **PostgreSQL avec le plugin natif.** Le connecteur PostgreSQL peut utiliser `pgoutput`, sans installer un plugin tiers sur le serveur. Il faut néanmoins configurer la réplication logique, les publications et les slots, et vérifier les possibilités offertes par le service PostgreSQL managé choisi.
3. **Plusieurs options côté Oracle.** Le connecteur Oracle de Debezium propose des modes de capture dont les prérequis, capacités et licences peuvent différer. Vérifier attentivement les versions, les architectures RAC/CDB/PDB, les types de données et les conditions d'utilisation Oracle.
4. **Fonctions CDC réutilisables.** Snapshots, reprise par offsets, transformations, filtrage, routage et outbox peuvent s'intégrer à la solution. La disponibilité et le comportement exacts dépendent du connecteur et de la version. La reprise est généralement à traiter avec une sémantique au moins une fois; prévoir l'idempotence des consommateurs.
5. **Ouverture et choix d'exécution.** Le projet Debezium est open source et s'utilise couramment avec Kafka Connect. Debezium Server et les intégrations de traitement de flux offrent d'autres architectures, mais ne signifient pas que toutes les destinations ou fonctions sont interchangeables.

Debezium est donc un candidat pertinent pour unifier Oracle et PostgreSQL, en particulier lorsque Kafka est déjà présent ou souhaité. Ce n'est pas une recommandation universelle: les contraintes Oracle, les volumes, le support éditeur, les compétences disponibles et le coût total peuvent conduire à un autre choix.

## 4. Nuances et risques à examiner

- **Oracle et très gros volumes.** Mesurer le débit et la charge sur des volumes réalistes. Le choix entre LogMiner, XStream ou un autre outil dépend des exigences, des licences et des contraintes d'exploitation. Certains DDL ou types volumineux peuvent présenter des restrictions.
- **PostgreSQL et slots de réplication.** Un slot non consommé peut empêcher le recyclage de WAL et faire croître l'espace disque. Surveiller le retard, l'état des slots et la rétention WAL.
- **Schéma et DDL.** La capture des modifications de lignes ne garantit pas la réplication automatique de toutes les opérations DDL. Vérifier explicitement le comportement des changements de schéma, renommages, suppressions et types non pris en charge.
- **Reprise et doublons.** Vérifier la persistance des offsets, les scénarios de redémarrage, l'ordre des événements et le comportement après réémission. Concevoir des consommateurs idempotents lorsque des doublons sont possibles.
- **Exploitation et support.** Kafka/Connect et les connecteurs nécessitent supervision, gestion des erreurs, mises à jour, sauvegardes et procédures de reprise. Un service managé peut réduire cette charge, au prix d'un périmètre de contrôle différent.
- **Licence et coût total.** Distinguer licence du logiciel, support, coûts cloud, infrastructure, stockage, trafic réseau et coûts d'exploitation. Confirmer les conditions propres aux éditions et fonctionnalités utilisées.

## 5. Orientation par contexte

| Contexte | Option à évaluer en premier | Point de vigilance |
| --- | --- | --- |
| Oracle et PostgreSQL, budget maîtrisé, Kafka présent ou souhaité | Debezium | Valider les versions, capacités Oracle et charge opérationnelle |
| Oracle critique, exigences élevées de débit ou de fonctions Oracle | Oracle GoldenGate; comparer aussi les autres modes Oracle | Coût, licences, tests de performance et support |
| Environnement principalement AWS ou Google Cloud avec peu d'équipe plateforme | AWS DMS ou Google Cloud Datastream, selon le cloud | Compatibilité réelle des sources/cibles, latence et limites du service |
| Priorité à une interface de réplication et des destinations analytiques | Qlik Replicate ou Fivetran HVR | Licence, SLA, connecteurs et coût total |
| Traitement et enrichissement en flux avec état | Debezium avec Kafka Streams/Flink, ou Flink CDC selon le besoin | Compétences, garanties de traitement et compatibilité des connecteurs |
| Capture Oracle spécialisée avec intégration Kafka | OpenLogReplicator ou Debezium Oracle | Support, prérequis et effort d'intégration |

## 6. Recommandation pour ce dépôt

Le dépôt actuel est un POC PostgreSQL vers PostgreSQL avec Debezium, Kafka Connect, Kafka et le JDBC sink. Il démontre la capture et la réplication de changements de lignes ainsi que la création dynamique de tables cibles. Il ne teste pas Oracle et ne démontre pas la haute disponibilité, les performances de production, la réplication de DDL ni les garanties de reprise à grande échelle.

Pour décider si Debezium convient également à un futur scénario Oracle, ajouter une validation séparée avec les versions et l'édition Oracle visées, les types de données réels, les exigences de DDL, le volume/retard cible, les licences, les scénarios de panne et le coût total. Voir aussi le [guide du POC CDC](cdc-process.md) et les [décisions d'architecture](adr/README.md).
