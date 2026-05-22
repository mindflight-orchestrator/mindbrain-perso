# MindBrain Syndic Ontology - Notes de versioning

## Principe

Chaque extraction de l'exemple syndic produit un `snapshot`. Les entites
(`domain_entity`), relations (`domain_relation`), scenarios et user stories sont
liees a ce snapshot. Une ancienne ligne n'est pas modifiee pour representer un
changement metier: le snapshot suivant contient le nouvel etat.

## Identifiants stables

Chaque objet porte un `logical_id` stable:

- `immeuble_residence_du_parc`
- `groupe_titulaires_immeuble_v1`
- `groupe_facturable_appartement_04_v1`
- `facturation_appartement_04_v1`

Le `content_hash` permet de comparer deux snapshots et de detecter si l'objet a
change.

## Groupes versionnes

Un groupe conceptuel reste stable, mais ses versions changent:

```text
groupe_facturable_appartement_04
  -> groupe_facturable_appartement_04_v1
  -> groupe_facturable_appartement_04_v2
```

Les factures ou charges doivent pointer vers la version applicable, jamais vers
le groupe conceptuel abstrait.

## Dates distinctes

Les relations importantes peuvent porter plusieurs dates:

- `valid_from` / `valid_to`
- `legal_effective_from` / `legal_effective_to`
- `billing_effective_from` / `billing_effective_to`

Exemple: un deces peut arreter la responsabilite de facturation a une date
donnee, sans clore immediatement la relation juridique de propriete tant que la
succession n'est pas formalisee.

## Requetes agent typiques

```sql
-- Qui possede l'immeuble ?
SELECT * FROM v_owner_group
WHERE owned_entity_id = 'immeuble_residence_du_parc'
  AND snapshot_id = latest_snapshot();

-- Quels menages occupent les appartements ?
SELECT * FROM v_active_occupancy
WHERE snapshot_id = latest_snapshot();

-- Quels groupes facturables sont actifs ?
SELECT * FROM v_current_billing_groups
WHERE snapshot_id = latest_snapshot();

-- Comment traiter un deces qui impacte la facturation ?
SELECT * FROM find_story('deces', 'facturation', 'groupe');
```

## Workflow d'import conceptuel

```text
1. Lire les YAML 01..06.
2. Inserer un snapshot.
3. Inserer chaque entite avec son logical_id et son content_hash.
4. Inserer chaque relation avec ses dates, proprietes et preuves.
5. Inserer les scenarios, actions, branches et user stories.
6. Recalculer les projections agent.
7. Comparer au snapshot precedent pour produire un diff.
```

## Difference avec le bundle MindBrain runtime

Ce dossier suit le format `story2doc` du CRM: il est optimise pour decrire,
versionner et interroger une connaissance metier. Il n'est pas encore un bundle
`ghostcrab_backup_bundle` directement importable par `mindbrain-standalone-tool
collection-import`. Une conversion dediee peut mapper:

- `domain_entity` -> `entities_raw`
- `domain_relation` -> `relations_raw`
- proprietes de relation -> `relation_properties_raw`
- user stories -> `documents_raw` ou projections dediees selon le runtime cible
