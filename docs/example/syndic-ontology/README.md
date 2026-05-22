# Syndic ontology story2doc format

Ce dossier applique au cas de l'immeuble le meme type de structure que
`mindbrain-story2doc/crm-ontology`.

## Fichiers

- `01_immeuble.yaml`: inventaire de l'immeuble, lots, annexes et parties communes.
- `02_menages.yaml`: personnes, cinq menages occupants et liens familiaux.
- `03_propriete.yaml`: groupe de titulaires et propriete du vieux couple.
- `04_occupation_facturation.yaml`: occupation des appartements et groupes facturables.
- `05_evenements_historique.yaml`: regles temporelles, preuves et changements futurs.
- `06_agent_queries.yaml`: user stories et requetes agent.
- `07_ontology_graph.yaml`: ontologie consolidee et graphe de scenarios.
- `08_ddl_mindbrain.sql`: DDL versionne inspire du format CRM.
- `09_versioning_notes.md`: notes de versioning et requetes types.

## Lecture

Les fichiers `01..06` sont des fragments scenario-first. Le fichier
`07_ontology_graph.yaml` consolide les types, relations, scenarios et user
stories. Le DDL `08_ddl_mindbrain.sql` fournit une forme persistable pour un
atelier MindBrain/Postgres, avec snapshots et diffs.

Ce format est volontairement distinct du fichier
`../syndic_ontology.yaml`, qui reste un vocabulaire plus compact. Ici, l'objectif
est de decrire le metier sous forme de scenarios versionnables et requetables
par agent.
