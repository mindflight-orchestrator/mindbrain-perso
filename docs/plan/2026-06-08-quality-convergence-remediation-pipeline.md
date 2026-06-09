# Quality Convergence and Remediation Pipeline

Date: 2026-06-08

## Objectif

Generaliser le processus initie dans Serenity P3 pour comparer les ontologies,
les schemas GhostCrab, les donnees graphes et les projections runtime, puis
produire des actions de remediation validables avant execution.

Le probleme traite est le suivant: les donnees importees dans le graphe peuvent
etre incompletes, mal qualifiees, ou partiellement desynchronisees des
ontologies natives et du registre de schemas. Le pipeline doit rendre ces ecarts
visibles, persistants et actionnables sans executer automatiquement de mutation
risquee.

## Design Global

Le design adopte une separation stricte entre trois niveaux:

- MindBrain natif: source technique du diagnostic, du stockage des runs et du
  cycle de vie des actions.
- GhostCrab MCP: orchestration assistant-facing, exposition des outils et
  validation humaine ou IA assistee.
- Domaines metier: regles, qualite attendue et remediation specifique, ajoutees
  ensuite au-dessus du socle natif.

La couche canonique par defaut est `native_ontology`. Les schemas GhostCrab, les
facettes et le graphe runtime sont compares a cette couche au lieu d'etre traites
comme des verites equivalentes.

## Ce Qui A Ete Developpe Dans MindBrain

### Stockage Natif

Deux tables ont ete ajoutees:

- `quality_convergence_run`: persiste chaque execution d'analyse.
- `quality_remediation_action`: persiste les actions proposees par une analyse.

Un run contient notamment:

- `workspace_id`
- `ontology_id`
- `run_kind`
- `canonical_layer`
- `input_fingerprint`
- `summary_json`
- `report_json`
- `status`

Une action contient notamment:

- `issue_type`
- `severity`
- `confidence`
- `reason`
- ancres optionnelles vers schema, entity type ou projection
- `evidence_json`
- `mcp_tool`
- `tool_args_json`
- `execution_mode`
- `status`
- decision, acteur et resultat d'execution

Les statuts d'action supportes sont:

- `proposed`
- `approved`
- `rejected`
- `applied`
- `failed`
- `skipped`

### Module Zig

Le module `src/standalone/quality_convergence.zig` centralise l'analyse.

Il compare actuellement:

- registre de schemas et definitions de facettes
- tables natives d'ontologie
- entites et relations du graphe
- rapport de couverture ontologique
- diagnostics graphe
- projections runtime disponibles

Il produit des actions pour les classes d'ecarts suivantes:

- divergence entre entites natives et schemas registres
- divergence entre edges natifs et granularite registry
- relations graphe non materialisees
- trous de couverture ontologique
- problemes remontes par les diagnostics graphes

### Surfaces HTTP

Les endpoints natifs ajoutes sont:

- `POST /api/mindbrain/quality/convergence/run`
- `GET /api/mindbrain/quality/convergence/runs`
- `GET /api/mindbrain/quality/convergence/run`
- `GET /api/mindbrain/quality/remediation/actions`
- `POST /api/mindbrain/quality/remediation/decision`
- `POST /api/mindbrain/quality/remediation/status`

Ces endpoints permettent de lancer une analyse, consulter les runs, consulter
les actions, approuver ou rejeter une action, puis enregistrer son resultat.

### CLI Standalone

Les commandes ajoutees sont:

- `quality-convergence`
- `quality-remediation-list`
- `quality-remediation-decision`
- `quality-remediation-status`

Elles donnent un acces direct au meme cycle de vie sans passer par le MCP.

### Export / Import Workspace

L'export complet workspace inclut maintenant:

- `quality_convergence_run`
- `quality_remediation_action`

Cela corrige le besoin de pouvoir sauvegarder et recharger l'etat d'analyse,
pas seulement les donnees metier ou les ontologies.

### Documentation

Un document de reference a ete ajoute:

- `docs/quality/convergence-pipeline.md`

Il decrit le stockage, les surfaces natives, le contrat d'analyse et la
politique de remediation.

## Ce Qui A Ete Developpe Dans GhostCrab MCP

GhostCrab expose le pipeline natif via six outils MCP:

- `ghostcrab_quality_convergence_run`
- `ghostcrab_quality_convergence_list`
- `ghostcrab_quality_convergence_get`
- `ghostcrab_quality_remediation_actions`
- `ghostcrab_quality_remediation_decide`
- `ghostcrab_quality_remediation_apply`

Le workflow cible est:

1. Lancer un run de convergence.
2. Lire le rapport persiste.
3. Lister les actions proposees.
4. Faire raffiner ou filtrer ces actions par l'IA.
5. Approuver explicitement les actions retenues.
6. Appliquer uniquement les actions supportees.
7. Relancer un run pour comparer l'etat apres remediation.

La premiere version de `apply` est volontairement limitee. Elle execute
uniquement les actions approuvees en mode `diagnostic_only` dont le tool propose
est `ghostcrab_graph_diagnostics`. Les autres actions restent inspectables et
decidables, mais ne sont pas executees automatiquement.

## Validation Effectuee

Validations MindBrain:

- `zig build standalone-tool --summary all --color off`: OK.
- `zig build test-standalone --summary all --color off -- --test-filter "quality convergence"`: OK, `309/309 tests passed`.

Validations GhostCrab:

- `pnpm run typecheck`: OK.
- `pnpm run build`: OK.
- `node dist/index.js tools list`: OK, catalogue a `62` outils incluant les six
  outils qualite.

Validation MCP runtime restante:

- `pnpm run verify:mcp-tools` echoue avec `MCP error -32000: Connection closed`.
- Le serveur MCP demarre bien en direct avec `62` outils, mais en mode degrade
  car le backend MindBrain HTTP local n'est pas disponible sur
  `http://127.0.0.1:8091`.

## Interpretation

Le pipeline cree une boucle d'amelioration qualite persistante:

- il observe les ecarts entre les couches
- il produit des actions idempotentes ou auditablement decidables
- il separe proposition, approbation et execution
- il laisse une trace exploitable par un assistant, un operateur ou un futur UI

Cette structure evite la double qualification implicite entre schemas GhostCrab
et ontologies natives. Le pipeline rend cette divergence explicite et mesurable,
au lieu d'essayer de la masquer dans un import ou une projection.

## Prochaines Etapes

- Ajouter un diff entre deux runs de convergence.
- Ajouter des handlers `apply` pour reindex, reimport controle et correction de
  qualification lorsque les contrats d'idempotence sont definis.
- Ajouter des rule packs metier au-dessus du socle natif.
- Ajouter une vue UI ou une queue operateur pour les actions proposees.
- Brancher le workflow sur un backend MindBrain actif pour valider le smoke MCP
  complet.
