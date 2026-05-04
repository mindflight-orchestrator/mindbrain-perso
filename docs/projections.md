# Projections

Projections are **agent-facing views of knowledge**. They are not the
primary source of truth. They are compact, scoped, ranked statements that
make the underlying ontology, facets, graph, memory, or program state
usable by an LLM or another agent.

In this project, projections sit next to the other ontology surfaces:

- **Ontologies** define the vocabulary: entity types, relation types,
  dimensions, and allowed values.
- **Facets** index attributes for filtering, search, aggregation, and
  hierarchy.
- **Directed graphs** model entities and typed relations for traversal.
- **Programs / agents** act on the modeled world.
- **Projections** expose a selected operational view of that world:
  facts, goals, steps, constraints, or other ranked context.

Short version: a projection is a purposeful shadow of the knowledge base,
optimized for context retrieval, auditability, and agent decisions.

## What a Projection Contains

The durable ontology projection table stores:

| Field | Meaning |
|-------|---------|
| `agent_id` | Agent or process that produced the projection. |
| `scope` | Workspace, domain, player, collection, or other boundary where the projection applies. |
| `proj_type` | Semantic type of the projection, such as `FACT`, `GOAL`, `STEP`, or `CONSTRAINT`. |
| `content` | Human/agent-readable statement, or structured text/JSON depending on the projection surface. |
| `weight` | Relative importance from `0` to `1`. |
| `source_ref` | Optional pointer back to the facet/source row that grounds the projection. |
| `source_type` | Optional label for the source kind, such as `taxonomy`, `document`, or entity type. |
| `status` | Lifecycle state: `active`, `resolved`, `expired`, or `blocking`. |
| `expires_at` | Optional expiry for temporary operational context. |

The memory pragma surface also uses projection-like rows such as
`canonical`, `proposition`, and `raw`. Those are compatible with the same
idea: they are ranked, searchable memory views used for context packing
and next-hop suggestions.

## What They Are Used For

Projections are used when the system needs a small, useful view instead of
the full underlying model.

Common uses:

- **Context packing**: select the most relevant facts, goals, steps, and
  constraints for an LLM prompt.
- **Operational audit**: record what an agent inferred, decided, or
  materialized after an ontology action.
- **Retrieval acceleration**: search compact projection text instead of
  traversing every facet and graph relation at request time.
- **Planning state**: expose active goals, ordered steps, and blocking
  constraints.
- **Derived ontology views**: turn taxonomy/facet/graph state into a
  readable statement an agent can consume.
- **Relevance scoring**: increase or decrease an entity/query score based
  on active projections in the same scope.

## Source of Truth vs. Projection

A projection should usually be derived from something else.

| Layer | Role | Example |
|-------|------|---------|
| Raw collection tables | Durable source data | A document chunk imported from a file. |
| Facets | Attribute/index layer | `source.extension = md`, `domain = physics`. |
| Graph | Relational layer | `ada -> works_for -> acme`. |
| Projection | Agent-facing view | `Ada works for Acme.` |

Do not treat a projection as the only copy of an important fact unless the
application explicitly models projections as the write surface. Prefer to
ground it with `source_ref`, a graph entity/relation, or a document/chunk
reference.

## How an LLM Should Create One

An LLM should not create a projection for every sentence it sees. It
creates one when a statement is useful as future operational context.

Use this decision process:

1. **Observe a candidate statement**

   The candidate may come from user input, a document chunk, a graph
   relation, a facet assignment, a previous agent action, or a program
   result.

2. **Ask whether it is operationally useful**

   Create a projection only if the statement is likely to help retrieval,
   planning, audit, or future decisions. Skip trivia, duplicated phrasing,
   unsupported guesses, and low-value restatements.

3. **Ground it**

   Prefer a projection that points back to evidence:

   - `source_ref` for a facet/document/chunk/source row.
   - graph entity/relation ids when the projection summarizes a graph edge.
   - a program/action id when the projection records an agent decision.

   If there is no grounding, the projection can still be created, but its
   `weight` should be lower and the content should make uncertainty clear.

4. **Choose the projection type**

   | Type | Use when | Example |
   |------|----------|---------|
   | `FACT` | The statement describes something believed true. | `Ada works for Acme.` |
   | `GOAL` | The statement describes a desired outcome. | `Project Alpha must be delivered on time.` |
   | `STEP` | The statement describes an action in a process. | `Call the account owner before sending the renewal offer.` |
   | `CONSTRAINT` | The statement limits, blocks, or governs action. | `Do not export private notes outside the workspace.` |

   In the pragma/memory surface, compatible types such as `canonical`,
   `proposition`, and `raw` may appear. A canonical memory row is usually a
   cleaned summary, a proposition row is structured DSL-like content, and a
   raw row preserves less-processed source text.

5. **Set the scope**

   Scope controls where the projection is visible. Use the narrowest scope
   that remains useful:

   - workspace for workspace-wide facts;
   - collection/domain for domain-specific facts;
   - entity/player/project scope for personalized or local context;
   - null/global scope only for truly cross-cutting context.

6. **Write concise content**

   The content should be directly useful to an agent. Prefer a clear
   sentence or a compact structured record. Avoid long passages, ambiguous
   pronouns, and hidden assumptions.

   Good:

   ```text
   Ada works for Acme.
   ```

   Better when structure matters:

   ```text
   fact|subject=ada|predicate=works_for|object=acme|conf=0.91
   ```

7. **Assign weight**

   Weight is not truth alone; it is retrieval importance.

   | Weight range | Meaning |
   |--------------|---------|
   | `0.8` - `1.0` | Highly relevant, well-grounded, should be surfaced often. |
   | `0.5` - `0.8` | Useful context with normal confidence or moderate priority. |
   | `0.2` - `0.5` | Weak, temporary, uncertain, or low-priority context. |
   | `< 0.2` | Usually not worth projecting unless needed for audit. |

8. **Set status**

   | Status | Use when |
   |--------|----------|
   | `active` | The projection should participate in retrieval/context packing. |
   | `blocking` | The projection is an active constraint or blocker and should be prioritized. |
   | `resolved` | The goal/constraint/step is no longer active but should remain auditable. |
   | `expired` | The projection is no longer valid because time or context moved on. |

9. **Deduplicate or update**

   Before creating a new projection, search for an existing active
   projection with the same scope, type, source, and meaning. Update weight,
   status, or content when the same projection already exists. Create a new
   row only when the meaning is materially different.

10. **Persist**

   Through GhostCrab, the MCP-level write is `ghostcrab_project`. In direct
   SQL or standalone paths, use the corresponding projection insert/update
   helper. Bulk ingestion should not call MCP for each row; it should use the
   direct database path and materialize projections in batches.

## LLM Creation Policy

An LLM should create a projection when all of the following are true:

- The statement is useful beyond the immediate response.
- The statement can be scoped.
- The statement can be typed as a fact, goal, step, or constraint.
- The statement is grounded or explicitly marked as uncertain.
- The statement is concise enough to be retrieved later.

An LLM should not create a projection when:

- The statement is merely a transient chat utterance with no future use.
- The statement duplicates an existing active projection.
- The statement is an unsupported inference presented as fact.
- The content belongs in raw documents, facets, or graph relations instead.
- The scope is unclear and creating a global projection would leak context
  across tenants/domains.

## Examples

### Fact from a Graph Relation

Graph relation:

```text
ada --works_for--> acme
```

Projection:

| Field | Value |
|-------|-------|
| `proj_type` | `FACT` |
| `scope` | `default` |
| `content` | `Ada works for Acme.` |
| `weight` | `0.9` |
| `source_type` | `graph_relation` |
| `status` | `active` |

### Goal from User Intent

User says:

```text
We need Project Alpha delivered before Friday.
```

Projection:

| Field | Value |
|-------|-------|
| `proj_type` | `GOAL` |
| `scope` | `project:alpha` |
| `content` | `Project Alpha must be delivered before Friday.` |
| `weight` | `0.9` |
| `source_type` | `user_request` |
| `status` | `active` |

### Constraint from Policy

Document says:

```text
Private notes must not be exported outside the workspace.
```

Projection:

| Field | Value |
|-------|-------|
| `proj_type` | `CONSTRAINT` |
| `scope` | workspace id |
| `content` | `Do not export private notes outside the workspace.` |
| `weight` | `1.0` |
| `source_ref` | document/chunk id |
| `source_type` | `policy_document` |
| `status` | `blocking` |

## Lifecycle

1. **Create** after an important fact, goal, step, or constraint is
   discovered.
2. **Use** during search, ranking, context packing, next-hop suggestions, or
   agent planning.
3. **Update** when better evidence, higher priority, or a new status is
   available.
4. **Resolve or expire** when the projection no longer applies.
5. **Audit** by following `agent_id`, `source_ref`, `source_type`, timestamps,
   and status changes.

## Design Rule

If facets and graphs describe the modeled world, projections describe the
part of that world an agent should currently remember, retrieve, or act on.

## Related: chunk-first pipeline

Ingestion can use an LLM to classify the document and drive deterministic
chunking before you attach per-chunk projections. That flow is documented in
[document-profile.md](document-profile.md) together with the `mindbrain-standalone-tool`
commands that persist to `documents_raw` and `chunks_raw`.
