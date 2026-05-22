# Exemple syndic: immeuble de 5 appartements

Ce document transforme la réflexion de `reflexion_chatgpt.md` en un exemple
ontologique court pour une agence de syndic. Le modèle reste un graphe: les
relations importantes sont qualifiées, datées et reliées à des preuves ou à des
événements plutôt que modifiées en place.

## Intentions du modèle

- Distinguer le patrimoine, l'occupation et la facturation.
- Représenter les groupes comme des identités stables avec des versions datées.
- Attacher les factures et responsabilités à la version applicable du groupe.
- Garder les causes de fin sous forme structurée: code, événement, document et
  note humaine.
- Ne jamais écraser une ancienne relation: un changement ajoute un nouvel état
  au graphe.

## Ontologies proposées

Le fichier `syndic_ontology.yaml` contient l'ontologie elle-même, séparée de
l'exemple d'immeuble. Il contient quatre modules:

- `syndic-building-core`: immeuble, lots privatifs, annexes et parties communes.
- `syndic-party-core`: personnes, ménages, groupes, versions et memberships.
- `syndic-rights-billing`: propriété, occupation, groupes facturables et règles
  de facturation.
- `syndic-events-evidence`: événements, documents, dates d'effet, statuts et
  causes de fin.

Le fichier `syndic_immeuble_5_appartements.yaml` est un graphe d'exemple qui
instancie cette ontologie avec 5 appartements, 5 ménages et leurs relations.

Une variante plus proche du format `mindbrain-story2doc/crm-ontology` est
disponible dans `syndic-ontology/`: scénarios YAML séparés, graphe d'ontologie
consolidé, DDL versionné et notes de versioning.

Ces modules sont volontairement séparés pour éviter de confondre:

- le graphe patrimonial: qui possède quoi;
- le graphe d'occupation: qui habite ou utilise quoi;
- le graphe de facturation: à quel groupe une charge est imputée;
- le graphe documentaire: quelle preuve confirme une relation;
- le graphe événementiel: quel événement crée, clôture ou remplace un état.

## Exemple métier

L'immeuble contient 5 appartements, 5 garages, 5 caves, 1 hall, 1 ascenseur et
1 espace machine à laver. Le vieux couple possède tout l'immeuble. Il occupe un
appartement, mais la propriété reste représentée par un groupe de titulaires
unique portant 100% de l'immeuble et de ses lots.

Les cinq ménages occupants sont:

- une célibataire;
- un célibataire;
- le vieux couple propriétaire;
- leur fils, sa femme et leurs deux enfants;
- un autre couple avec deux enfants.

Dans cet exemple, "vieux couple sans enfants" signifie sans enfant vivant dans
leur appartement. Ils ont bien un fils adulte, modélisé comme membre d'un autre
ménage et relié à eux par une relation familiale.

## Règle de lecture

Les relations `POSSEDE`, `OCCUPE`, `MEMBRE_DE` et `FACTURABLE_POUR` portent des
dates, un statut, des quotes-parts ou une règle. Si un décès, une vente, un
divorce ou une mutation successorale survient, l'ancien état reste historisé et
une nouvelle version est créée.

Exemple de principe:

```text
GroupeFacturable_Appartement_3
  aVersion -> GroupeFacturable_Appartement_3_v1

Facture_Mars_2026
  factureeA -> GroupeFacturable_Appartement_3_v1
```

Une facture passée ne pointe donc jamais vers un groupe abstrait modifié après
coup. Elle pointe vers la version du groupe applicable au moment du fait
générateur.
