---
title: Bundled BM25 Stopwords
---

# Bundled BM25 Stopwords

`data/*.csv` is the repo-local copy of the stopword lists used to seed the
SQLite `bm25_stopwords` table. Each CSV has a single `stop_word` header column.

`seed_bm25_stopwords.sql` is the generated, idempotent default seed. The
generator writes the canonical SQL copy here and the embedded standalone copy at
`src/standalone/seed_bm25_stopwords.sql`. SQLite bootstrap applies the embedded
copy automatically and refreshes the bundled rows using
`source = 'bundled:<filename>'`.

Regenerate the file with `scripts/generate-stopwords-seed.mjs` after editing
any CSV in `sql/stopwords/data/`.

The generated seed splits each CSV row into normalized lexemes using the same
word-boundary rules as the SQLite tokenizer. Multi-word entries therefore seed
multiple rows in `bm25_stopwords`, one per normalized token.
