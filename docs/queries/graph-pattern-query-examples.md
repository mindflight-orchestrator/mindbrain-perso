# Graph Pattern Query Examples

These examples are the SQLite-first syntax catalogue for GPQ. They are based on
the real `data/immeuble-demo.sqlite` graph where possible, with a micro-fixture
used by tests for Type B projection bundles and tightly controlled edge cases.

## Real Immeuble Examples

### Unit by metadata

```text
WORKSPACE immeuble-demo
MATCH (u:unit)
WHERE u.metadata.building_id = 1
PROJECT u.entity_id, u.name, u.metadata.lot, u.metadata.usage_status
LIMIT 20
```

Expected examples include Tilleuls units such as `Tilleuls Appartement A3`.

### Unit by inline name

```text
WORKSPACE immeuble-demo
MATCH (u:unit {name: 'Tilleuls Appartement A3'})
PROJECT u.entity_id, u.name, u.metadata.floor, u.metadata.lot
LIMIT 1
```

### Ownership with numeric typed relation property

```text
WORKSPACE immeuble-demo
MATCH (p:person)-[o:owns]->(u:unit)
WHERE o.prop.quote_part >= 0.5
PROJECT p.name, u.name, o.relation_id, o.prop.quote_part
LIMIT 20
```

`quote_part` is the real property in `immeuble-demo`; use it instead of the
older illustrative `share_bp` name.

### Ownership with text typed relation property

```text
WORKSPACE immeuble-demo
MATCH (p:person)-[o:owns]->(u:unit)
WHERE o.prop.right_type = 'pleine_propriete'
PROJECT p.name, u.name, o.prop.right_type
LIMIT 20
```

### Lease/rent with money_minor typed relation property

```text
WORKSPACE immeuble-demo
MATCH (org:organization)-[r:rented_to]->(h:household)
WHERE r.prop.monthly_rent >= 100000
PROJECT org.name, h.name, r.prop.monthly_rent
LIMIT 20
```

### Building traversal

```text
WORKSPACE immeuble-demo
MATCH (b:building {name: 'Résidence Les Tilleuls'})-[r:contains]->(x:unit)
HOPS 1..2
WHERE r.relation_type IN ('contains')
PROJECT x.entity_id, x.entity_type, x.name
LIMIT 50
```

This covers building -> block -> unit traversal.

## Micro-Fixture Examples

### Type B ProjectionResult bundle

```text
WORKSPACE seo-audit
MATCH (pr:ProjectionResult)
WHERE pr.metadata.projection_id = 'proj_keyword_opportunities'
PROJECT BUNDLE projection_get
LIMIT 1
```

This syntax is tested with a controlled in-memory fixture. It is not expected to
return data from `immeuble-demo`, whose golden bundle does not contain Type B
`ProjectionResult` rows.

## Negative Syntax Cases

The parser tests cover:

- missing `WORKSPACE`;
- invalid `HOPS 3..1`;
- invalid `LIMIT 0`;
- unsupported bundle names;
- malformed `MATCH` patterns.
