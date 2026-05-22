# Audit global GhostCrab MCP / MindBrain

## Synthese

GhostCrab MCP n'importe pas directement une ontologie YAML via le serveur MCP. Le flux reel se compose de trois couches :

1. `gcp` orchestre le lancement du backend MindBrain et du serveur MCP.
2. Le MCP TypeScript expose les tools et ecrit via HTTP/SQL vers le backend.
3. MindBrain standalone possede le fichier SQLite, applique le schema et execute les imports natifs.

Le fichier SQLite est resolu par `gcp` avec la priorite suivante : `GHOSTCRAB_SQLITE_PATH`, option CLI `--db`, configuration de workspace, puis fallback local. Le backend recoit ensuite ce chemin via `GHOSTCRAB_SQLITE_PATH` et toutes les ecritures finissent dans cette base.

## Chemins d'import

### Import ontologie RDF

`gcp brain ontology import` importe du RDF/N-Triples, pas le YAML documentaire de `docs/ghostcrab-ontologie`. Il alimente la couche ontologie :

- `ontologies`
- `ontology_triples_raw`
- `ontology_entity_types`
- `ontology_edge_types`
- `ontology_entities_raw`
- `ontology_relations_raw`

Avec `--materialize-graph`, l'import ecrit aussi dans les tables raw du graphe operationnel :

- `entities_raw`
- `relations_raw`

Ces tables raw peuvent ensuite etre projetees vers les tables runtime avec `ghostcrab_graph_reindex`.

### Import bundle GhostCrab

`gcp brain load <bundle.json>` importe un `ghostcrab_backup_bundle`. C'est le format le plus adapte pour charger ensemble :

- workspace
- collections
- ontologies
- taxonomies
- documents et chunks
- facet assignments
- entites raw
- relations raw
- proprietes de relations
- liens entite-document et entite-chunk

Pour un graphe local complet, c'est le format cible le plus utile, car il peut transporter a la fois le modele et les donnees.

### Import documents

`gcp brain document ...` couvre le chemin corpus/documentaire. Il sert a normaliser, profiler, importer et qualifier des documents. Ce flux alimente notamment :

- `documents_raw`
- `chunks_raw`
- `facet_assignments_raw`
- eventuellement des liens vers entites et chunks

Ce chemin est distinct d'un import pur d'ontologie.

### Registry local

Les commandes de type `gcp ontologies pull/show` gerent des ressources locales de registry. Elles ne doivent pas etre confondues avec un import natif en base MindBrain.

## Stockage

### Facts et memoire

La table `facets` stocke les facts, schemas et enregistrements generiques utilises par GhostCrab. Les tools comme `ghostcrab_remember`, `ghostcrab_upsert` et `ghostcrab_schema_register` ecrivent ici.

Chaque ligne porte typiquement :

- `schema_id`
- `content`
- `facets_json`
- `workspace_id`
- `source_ref`
- `doc_id`
- eventuellement un embedding

### Ontologie et taxonomie

Les tables `ontology_*` representent la couche de definition :

- ontologies
- namespaces
- dimensions
- valeurs
- types d'entites
- types de relations
- triples preserves
- entites et relations ontologiques raw

Cette couche decrit le domaine. Elle ne remplace pas automatiquement le graphe runtime tant qu'elle n'est pas materialisee ou reindexee.

### Donnees raw

Les tables raw constituent la source canonique importee pour un workspace :

- `entities_raw`
- `entity_aliases_raw`
- `relations_raw`
- `relation_properties_raw`
- `entity_documents_raw`
- `entity_chunks_raw`

Elles permettent de conserver les donnees importees et de regenerer les tables derivees.

### Graphe runtime

Les tables `graph_*` sont les tables derivees utilisees par les tools de recherche, traversal et graphe :

- `graph_entity`
- `graph_entity_alias`
- `graph_relation`
- `graph_relation_property`
- `graph_entity_document`
- `graph_entity_chunk`

Elles peuvent etre ecrites directement par certains tools, comme `ghostcrab_learn`, ou reconstruites depuis les tables raw avec `ghostcrab_graph_reindex`.

### Semantique de tables

Les tables suivantes decrivent la semantique DDL/workspace, pas les donnees du graphe elles-memes :

- `table_semantics`
- `column_semantics`
- `relation_semantics`

Elles servent a decrire comment des tables ou colonnes doivent etre comprises, mappees ou exploitees.

## Projections

Dans GhostCrab, une projection n'est pas l'ontologie elle-meme. C'est une vue compacte de travail, preparee pour un agent.

Une projection est stockee dans `projections` avec notamment :

- `agent_id`
- `scope`
- `proj_type`
- `content`
- `weight`
- `source_ref`
- `source_type`
- `status`

Les types principaux sont :

- `FACT`
- `GOAL`
- `STEP`
- `CONSTRAINT`
- `NOTE`

Le role d'une projection est de repondre a la question : quel petit paquet de contexte doit etre donne a l'agent maintenant ?

Exemples :

- `FACT` : etat vrai et utile.
- `GOAL` : objectif courant.
- `STEP` : prochaine action ou etape de travail.
- `CONSTRAINT` : contrainte ou blocage, potentiellement prioritaire.
- `NOTE` : contexte brut ou moins structure.

`ghostcrab_project` cree ou rafraichit ces projections. `ghostcrab_pack` les combine avec les facts pertinents pour produire un pack compact de memoire de travail. Les contraintes bloquantes sont prioritaires dans ce pack.

Il existe aussi un sens plus technique du mot projection dans le code : la materialisation de tables raw vers des tables derivees, par exemple `relation_properties_raw` vers `graph_relation_property`. Ce sens concerne la maintenance interne du graphe. Le sens important cote GhostCrab MCP est la projection agent-ready.

## Application a l'exemple de l'immeuble

Pour l'exemple d'un immeuble avec cinq appartements, le format cible recommande est un `ghostcrab_backup_bundle`, pas le YAML documentaire de `docs/ghostcrab-ontologie`.

Le bundle devrait contenir :

- un workspace, par exemple `immeuble-demo`
- une ontologie, par exemple `immeuble-demo::core`
- des types d'entites :
  - `building`
  - `apartment`
  - `hall`
  - `elevator`
  - `garage`
  - `cellar`
  - `laundry_room`
  - `person`
  - `household`
- des types de relations :
  - `contains`
  - `owns`
  - `occupies`
  - `has_member`
  - `spouse_of`
  - `parent_of`
  - `assigned_garage`
  - `assigned_cellar`
- des `entities_raw` pour :
  - l'immeuble
  - le hall
  - l'ascenseur
  - les cinq appartements
  - les cinq garages
  - les cinq caves
  - l'espace machine a laver
  - la celibataire
  - le celibataire
  - le vieux couple proprietaire
  - les deux couples avec deux enfants
- des `relations_raw` pour :
  - la composition du batiment
  - la propriete du vieux couple
  - l'occupation des appartements
  - les relations familiales
  - le lien entre le fils, sa femme et le vieux couple
  - les affectations garage/cave

Le flux d'utilisation serait :

1. generer le `ghostcrab_backup_bundle`
2. charger le bundle avec `gcp brain load <bundle.json>`
3. executer `ghostcrab_graph_reindex`
4. interroger avec `ghostcrab_graph_search`, `ghostcrab_combined_search` ou `ghostcrab_pack`

Les projections utiles pour cet exemple pourraient etre :

- `FACT` : le vieux couple possede tout l'immeuble.
- `FACT` : un des couples avec enfants est le fils du vieux couple et sa femme.
- `STEP` : associer chaque appartement a un garage et une cave.
- `CONSTRAINT` : aucun bail, reglement de copropriete ou quote-part n'est encore modelise.
- `GOAL` : obtenir une vue complete propriete, occupation et usage des espaces communs.

## Conclusion

GhostCrab MCP expose les outils d'acces et de modelisation, mais l'import et le stockage durable sont portes par MindBrain standalone et son schema SQLite. Pour un graphe local contenant taxonomie, ontologie et donnees, le meilleur format d'echange est le `ghostcrab_backup_bundle`. Les projections viennent ensuite comme couche de travail agent-ready : elles ne remplacent pas le graphe, elles en extraient un contexte compact, priorise et actionnable.
