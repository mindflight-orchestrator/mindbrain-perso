# Native projection contracts v1

Qualified live projections use a `projection_contract` object in the existing
answer-artifact definition. The authoritative typed structure is
`src/standalone/projection_contract.zig`; unknown contract fields and unsupported
versions are rejected. No new tables or migrations are introduced.

The three bounded operations are `missing_inbound`, `group_sum`, and
`dated_relations`. They execute over `entities_raw` and `relations_raw`, filtered
by both workspace and explicit ontology before joining or aggregating. Graph
cache rows are not silently treated as canonical ontology-scoped records.

Creation validates the bound ontology/version, required native entity types,
column concepts, relation declaration and contract shape. Native predicate to
ontology predicate bindings are explicit. Concrete source types in
`member_types` are declared mappings; this implementation does not infer missing
OWL inheritance or establish that an arbitrary business contract is correct.
Contracts still need independent business acceptance before use.

`POST /api/mindbrain/ghostcrab/artifact/{id}/refresh` executes the contract and
stores `qualified_result` atomically with the version and refresh event. Missing
numeric data, invalid temporal data, missing required fields/relations, mismatched
quota scales and invalid source references cannot yield a complete ready result.
An empty dataset is interpreted only within `declared_records_only` coverage.
`qualified_result`, like `materialized`, is reserved against caller-supplied data.

`GET /api/mindbrain/ghostcrab/artifact/{id}` adds nullable `answer_json`. For a
qualified projection, the read takes one SQLite read snapshot and verifies the
stored contract digest, source digest and ontology version. Source or definition
changes return a stale status without result rows in `answer_json`. Reads do not
refresh, repair indexes, or consult `agent_facts`. Legacy artifacts keep their
existing raw payload behavior; consumers must use `answer_json` for qualified
serving instead of treating old `payload_json` materializations as fresh.

The result includes scoped native entity/relationship evidence IDs, relevant
ontology definitions, column explanations, explicit assumptions, as-of date,
computation timestamp, completeness and digests. IDs are interpreted together
with the result's workspace and ontology, not as globally portable references.

Limits: 200 output rows, 256 KiB serialized qualified result, 5,000 source records
per native entity/relation table, 64 KiB per source record, 4 MiB total hashed
source bytes and 128 KiB native ontology description. Exceeding limits fails;
there is no silent successful truncation. Conservative invalidation hashes the
bound ontology's source records and native entity/edge definitions. It is suitable
for this bounded pilot; larger catalogs need a separately qualified revision
mechanism and execution limits.

Relation validity dates are inclusive. `dated_relations` uses inclusive start/end
fields at the explicit as-of date, treats a missing end as open-ended, excludes
`cancelled`, `terminated` and `inactive` statuses, and treats unknown statuses or
invalid intervals as indeterminate. Source field names and status semantics must
match the prepared business contract. Current scope does not implement arbitrary
SQL, general semantic question matching, historical source reconstruction, or
full OWL constraint reasoning.

Validation: `zig build test` includes independent totals, relation mutation,
missing evidence, date intervals, quota units, cross-workspace/ontology checks,
size limits and a SQLite authorizer that forbids `agent_facts` reads.
