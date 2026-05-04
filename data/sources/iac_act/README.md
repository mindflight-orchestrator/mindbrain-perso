# IAC / EU AI Act–style fixtures

This folder holds **small, deterministic samples** used by unit tests (`data_sources_test.zig`). They mimic the structure of Regulation (EU) 2024/1689 (AI Act)—recitals, articles, annex-style sections—without embedding the full official text.

To ingest the real act, add `*.md` or `*.html` exports from EUR-Lex here and point your importer at those paths; the same `data/sources/<name>/` layout applies.
