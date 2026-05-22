# SOP Start Reference

J'ai cree l'artefact d'ontologies ici :
`docs/dev/SOP_start/reference_ghostcrab_ontologies.yaml`.

Il contient deux ontologies explicites :

- `ghostcrab-reference-meta` : l'ontologie interne GhostCrab, avec activity families, modeling/projection recipes, intent/signal patterns, capabilities, autonomy policies, tool catalog et typed facets.
- `ghostcrab-loadout-starters` : les 7 loadouts du code source, dont les 6 visibles sur l'image plus `workflow-tracking` qui existe dans `src/db/ontology-loadouts.ts`.

J'ai aussi ajoute un `registration_plan` qui mappe ces ontologies vers les outils reels :
`ghostcrab_schema_register`, `ghostcrab_ontology_register_entity_type`,
`ghostcrab_ontology_register_relation_type`, `ghostcrab_loadout_seed`,
`ghostcrab_typed_facet_upsert/query`, et `ghostcrab_tool_search`.

Validation faite : le fichier YAML parse correctement avec la dependance `yaml`
du projet (`2` ontologies, `7` loadouts).
