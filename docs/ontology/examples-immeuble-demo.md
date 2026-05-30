# Ontology Example: `immeuble-demo`

This page records the current ontology state of `data/immeuble-demo.sqlite` in
this checkout.

## Table Counts

```text
table_name              rows
----------------------  ----
ontologies              2
ontology_namespaces     4
ontology_dimensions     19
ontology_values         11
ontology_entity_types   24
ontology_edge_types     33
ontology_entities_raw   5
ontology_relations_raw  4
ontology_triples_raw    182
collection_ontologies   1
workspace_settings      1
```

## Ontology Catalog

```text
ontology_id             workspace_id   name     version  source_kind  frozen
----------------------  -------------  -------  -------  -----------  ------
immeuble-demo::core     immeuble-demo  core     2.1.0    linkml       0
immeuble-demo::default  immeuble-demo  default  1.0.0    auto         0
```

## Sample Dimensions

```text
namespace  dimension        value_type  is_multi  hierarchy_kind
---------  ---------------  ----------  --------  --------------
domain     building         string      1         flat
domain     decision         string      1         flat
domain     role             string      1         flat
domain     scenario         string      1         flat
domain     status           string      1         flat
domain     unit             string      1         flat
finance    charge_status    string      1         flat
finance    payment_status   string      1         flat
immeuble   LifecycleStatus  string      0         flat
immeuble   PaymentStatus    string      0         flat
source     document_type    string      0         flat
```

## Sample Values

```text
namespace  dimension        value_id  value                 parent_value_id  label
---------  ---------------  --------  --------------------  ---------------  --------------------
immeuble   LifecycleStatus  1         confirmed                              confirmed
immeuble   LifecycleStatus  2         pending_verification                   pending_verification
immeuble   LifecycleStatus  3         contested                              contested
immeuble   LifecycleStatus  4         closed                                 closed
immeuble   PaymentStatus    1         expected                               expected
immeuble   PaymentStatus    2         matched                                matched
immeuble   PaymentStatus    3         complete                               complete
immeuble   PaymentStatus    4         partial                                partial
immeuble   PaymentStatus    5         manual_review                          manual_review
immeuble   PaymentStatus    6         overdue                                overdue
immeuble   PaymentStatus    7         closed                                 closed
```

## Sample Entity Types

```text
entity_type     label
--------------  ----------------------------------------------------------------------------
bank_account    bank_account
billing_group   Versioned group used as the operational recipient of charges.
block           Physical block or stairwell inside a building.
building        Residential or mixed building managed by a syndic.
cellar          cellar
charge_call     charge_call
coda_entry      Bank statement line imported from a Belgian CODA file.
decision        decision
document        document
event           event
household       Residential household occupying a lot; members remain individual persons.
lease_contract  Lease contract linking a landlord, a tenant household, and a unit over time.
```

## Sample Edge Types

```text
edge_type                           source_entity_type  target_entity_type
----------------------------------  ------------------  ------------------
allocated_to                        coda_entry          billing_group
assigned_cellar                     unit                cellar
assigned_garage                     unit                parking_space
bills_to                            billing_group       unit
block_contains_unit                 block               unit
building_contains_block             building            block
building_contains_cellar            building            cellar
building_contains_parking_space     building            parking_space
building_contains_private_garden    building            private_garden
building_contains_shared_equipment  building            shared_equipment
building_contains_shared_space      building            shared_space
building_contains_unit              building            unit
```

## Reproduce

```bash
sqlite3 -header -column data/immeuble-demo.sqlite "
SELECT 'ontologies' AS table_name, COUNT(*) AS rows FROM ontologies
UNION ALL SELECT 'ontology_namespaces', COUNT(*) FROM ontology_namespaces
UNION ALL SELECT 'ontology_dimensions', COUNT(*) FROM ontology_dimensions
UNION ALL SELECT 'ontology_values', COUNT(*) FROM ontology_values
UNION ALL SELECT 'ontology_entity_types', COUNT(*) FROM ontology_entity_types
UNION ALL SELECT 'ontology_edge_types', COUNT(*) FROM ontology_edge_types
UNION ALL SELECT 'ontology_entities_raw', COUNT(*) FROM ontology_entities_raw
UNION ALL SELECT 'ontology_relations_raw', COUNT(*) FROM ontology_relations_raw
UNION ALL SELECT 'ontology_triples_raw', COUNT(*) FROM ontology_triples_raw
UNION ALL SELECT 'collection_ontologies', COUNT(*) FROM collection_ontologies
UNION ALL SELECT 'workspace_settings', COUNT(*) FROM workspace_settings;
"
```
