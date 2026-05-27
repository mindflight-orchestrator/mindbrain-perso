## Tests exécutés

Backend **mindbrain-http** sur `http://127.0.0.1:8092` avec `data/immeuble-demo.sqlite` :

| Test | Résultat |
|------|----------|
| `bash scripts/demo-immeuble-gaps.sh` (HTTP) | OK — 4 rules importées, 0 violation métier |
| `--simulate-anomaly` | OK — `unit-one-cellar` détectée sur entity 18 |
| `curl /api/mindbrain/coverage?workspace_id=immeuble-demo` | OK — voir explication ci-dessous |
| `zig build test-standalone` | **All unit tests passed** |

Golden data après rules : `missing_required_relations: 0`, `cardinality_violations: 0`, ~50 `relation_type_mismatch` (schema vs instance).

---

## `ghostcrab_coverage` — pourquoi `0 / 0` ?

Ce n’est **pas** un bug sur immeuble-demo : c’est une **absence de matière première** pour l’algorithme.

### Ce que coverage compare réellement

Le moteur ([`ontology_sqlite.zig`](src/standalone/ontology_sqlite.zig)) fait :

1. Lire les lignes **`facets`** du workspace avec `schema_id` ∈ `{ ghostcrab:ontology, mindbrain:ontology, ghostcrab:taxonomy }`
2. Pour chaque nœud ontologie/taxonomie dans ces facets, chercher une entité **`graph_entity`** correspondante (par `node_id` / nom)
3. Produire `total_nodes`, `covered_nodes`, `gaps[]`

```806:810:src/standalone/ontology_sqlite.zig
fn isOntologyOrTaxonomy(schema_id: []const u8) bool {
    return std.mem.eql(u8, schema_id, "mindbrain:ontology") or
        std.mem.eql(u8, schema_id, "ghostcrab:ontology") or
        std.mem.eql(u8, schema_id, "ghostcrab:taxonomy");
}
```

### État immeuble-demo

| Table | Contenu |
|-------|---------|
| `ontology_entity_types` | **24** types (building, unit, charge_call…) |
| `graph_entity` | **131** instances |
| **`facets`** (ontology/taxonomy) | **0** |

Le bundle charge l’ontologie dans `ontology_*` et le graphe dans `graph_*`, **sans** miroir facet `ghostcrab:ontology`. Donc :

- `facet_rows: 0`
- `total_nodes: 0` → `coverage_ratio: null`
- `gaps[]` vide

`graph_entities: 131` indique seulement qu’il y a des instances — pas qu’elles couvrent un catalogue ontologie facet.

### Différence avec `graph_gap_rules` / diagnostics

| Outil | Question |
|-------|----------|
| **`ghostcrab_coverage`** | « Chaque **nœud du catalogue ontologie** (facets) a-t-il une instance dans le graphe ? » |
| **`graph_gap_rules`** | « Chaque **instance** respecte-t-elle nos invariants métier (closed world) ? » |
| **`graph_diagnostics`** | Rules + topo + evidence + **réutilise** coverage pour `ontology_coverage_gap` |

Sur immeuble, `ontology_coverage_gaps: 0` dans diagnostics pour la même raison : **0 gaps à énumérer** quand `total_nodes = 0`.

Pour rendre coverage utile sur immeuble, il faudrait **matérialiser** l’ontologie en facets (reindex ontologie → `facets`, ou étape Studio/GhostCrab dédiée), puis comparer les 24 types / dimensions taxonomiques aux 131 entités.

---

## Règles métier syndic supplémentaires — oui, c’est le bon levier

Les `graph_gap_rules` actuelles couvrent surtout **structure patrimoniale** (cave, garage, rattachement immeuble). On peut enrichir par domaine syndic, en s’appuyant sur les arêtes réelles du demo :

### Patrimoine & lots (déjà partiellement couvert)

| rule_id proposée | Règle | Golden demo |
|------------------|-------|-------------|
| `unit-one-cellar` | ✓ déjà | 13/13 |
| `unit-in-building` | ✓ déjà (`contains` entrant) | 13/13 |
| `unit-primary-residence` | chaque `unit` → exactement 1 `primary_residence_of` → `household` | 13/13 probable |
| `ground-floor-garden` | unit RDC → ≥1 `uses_exclusive` → `private_garden` | 6 jardins — à calibrer |

### Occupants & ménages

| rule_id | Règle | Nuance |
|---------|-------|--------|
| `household-has-member` | chaque `household` → ≥1 `household_member` → `person` | **12/13** — le lot « Érables A4 vacant travaux » n’a **volontairement** pas de membre → rule trop stricte sans exception `status=vacant` |
| `occupied-unit-has-occupant` | `unit` avec `occupies` entrant ≥1 | 27 personnes occupent, 13 lots — certains lots multi-occupants |
| `person-not-isolated` | `person` → degree ≥1 | détecte **Marie Lambert** (déjà natif `isolated_entity`) |

### Propriété & baux

| rule_id | Règle | Golden |
|---------|-------|--------|
| `owned-unit-has-owner` | `unit` avec `owns` entrant ≥1 | 13/13 units_with_owns |
| `leased-unit-has-lease` | `unit` avec `leases` entrant ≥1 | 5/5 — **déjà en JSON, désactivée** ; activer pour lots loués |
| `lease-has-tenant` | `lease_contract` → ≥1 `rented_to` → `person` | à vérifier sur 5 baux |

### Facturation & CODA (scénarios finance)

| rule_id | Règle | Scénario YAML |
|---------|-------|---------------|
| `billing-group-bills-unit` | `billing_group` → ≥1 `bills_to` → `unit` | 13/13 |
| `coda-complete-matched` | `coda_entry` soldé → ≥1 `matched_to` | `scenario:coda-complete-payment` |
| `coda-partial-review` | paiement partiel → `requires_review` | `scenario:coda-partial-reminder` |
| `charge-call-allocated` | `charge_call` → `allocated_to` | appels de charges |

Exemple JSON (extrait à ajouter à `gap-rules.demo.json` ou fichier `gap-rules.syndic.json`) :

```json
{
  "rule_id": "household-has-member",
  "entity_type": "household",
  "relation_type": "household_member",
  "direction": "out",
  "target_entity_type": "person",
  "min_count": 1,
  "severity": "warning",
  "label": "Ménage sans membre identifié"
},
{
  "rule_id": "owned-unit-has-owner",
  "entity_type": "unit",
  "relation_type": "owns",
  "direction": "in",
  "target_entity_type": "person",
  "min_count": 1,
  "severity": "error",
  "label": "Lot sans copropriétaire (owns entrant)"
},
{
  "rule_id": "billing-group-bills-unit",
  "entity_type": "billing_group",
  "relation_type": "bills_to",
  "direction": "out",
  "target_entity_type": "unit",
  "min_count": 1,
  "severity": "error",
  "label": "Groupe de facturation sans lot cible"
}
```

### Limites actuelles des rules unary

- Pas de filtre sur **statut** (vacant, loué, en travaux) → rules comme `household-has-member` créent du bruit métier légitime.
- Pas d’**agrégats** (quotités = 1000 par immeuble) → hors `min_count`/`max_count` sur relations.
- Pas de **chaînes** (bail → lot → locataire → charges) → roadmap **§11 motifs**.

**Recommandation** : ajouter d’abord les rules **sans faux positifs** sur golden (`owned-unit-has-owner`, `billing-group-bills-unit`, activer `leased-unit-has-lease` ciblée), puis les rules « conditionnelles » quand §11 ou metadata sur `graph_entity` permet d’exclure « vacant travaux ».

---

Le backend tourne encore sur **8092** (PID dans `/tmp/mindbrain-http-8092.pid`). Pour arrêter : `kill $(cat /tmp/mindbrain-http-8092.pid)`.

Si tu veux, je peux créer un `gap-rules.syndic.json` séparé avec les rules validées sur golden et l’intégrer au script de démo.