#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");
const outputPaths = [
  resolve(repoRoot, "sql/stopwords/seed_bm25_stopwords.sql"),
  resolve(repoRoot, "src/standalone/seed_bm25_stopwords.sql")
];

const stopwordSources = [
  { language: "simple", csv: "stopwords_arabe.csv" },
  { language: "simple", csv: "stopwords_catalan.csv" },
  { language: "danish", csv: "stopwords_da.csv" },
  { language: "german", csv: "stopwords_de.csv" },
  { language: "english", csv: "stopwords_en.csv" },
  { language: "finnish", csv: "stopwords_fi.csv" },
  { language: "french", csv: "stopwords_fr.csv" },
  { language: "english", csv: "stopwords_fr_en_nl.csv" },
  { language: "french", csv: "stopwords_fr_en_nl.csv" },
  { language: "dutch", csv: "stopwords_fr_en_nl.csv" },
  { language: "simple", csv: "stopwords_gr.csv" },
  { language: "hungarian", csv: "stopwords_hu.csv" },
  { language: "italian", csv: "stopwords_it.csv" },
  { language: "simple", csv: "stopwords_jp.csv" },
  { language: "dutch", csv: "stopwords_nl.csv" },
  { language: "norwegian", csv: "stopwords_no.csv" },
  { language: "simple", csv: "stopwords_pl.csv" },
  { language: "portuguese", csv: "stopwords_pt.csv" },
  { language: "russian", csv: "stopwords_ru.csv" },
  { language: "spanish", csv: "stopwords_sp.csv" },
  { language: "swedish", csv: "stopwords_sw.csv" },
  { language: "turkish", csv: "stopwords_tu.csv" }
];

function isWordChar(byte) {
  return (
    byte >= 0x80 ||
    (byte >= 0x30 && byte <= 0x39) ||
    (byte >= 0x41 && byte <= 0x5a) ||
    (byte >= 0x61 && byte <= 0x7a)
  );
}

function lowerAsciiByte(byte) {
  if (byte >= 0x41 && byte <= 0x5a) {
    return byte + 0x20;
  }
  return byte;
}

function tokenizeLine(line) {
  const bytes = Buffer.from(line, "utf8");
  const tokens = [];

  let index = 0;
  while (index < bytes.length) {
    while (index < bytes.length && !isWordChar(bytes[index])) {
      index += 1;
    }
    if (index >= bytes.length) {
      break;
    }

    const start = index;
    while (index < bytes.length && isWordChar(bytes[index])) {
      index += 1;
    }

    const tokenBytes = Buffer.allocUnsafe(index - start);
    for (let tokenIndex = 0; tokenIndex < tokenBytes.length; tokenIndex += 1) {
      tokenBytes[tokenIndex] = lowerAsciiByte(bytes[start + tokenIndex]);
    }

    const token = tokenBytes.toString("utf8").trim();
    if (token.length > 0) {
      tokens.push(token);
    }
  }

  return tokens;
}

function escapeSqlLiteral(value) {
  return value.replace(/'/g, "''");
}

function chunk(values, size) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

async function loadRowsForSource(source) {
  const csvPath = resolve(repoRoot, "sql/stopwords/data", source.csv);
  const csvText = await readFile(csvPath, "utf8");
  const lines = csvText.replace(/\r\n/g, "\n").split("\n");
  const rows = [];
  const seen = new Map();

  for (const rawLine of lines.slice(1)) {
    const line = rawLine.trim();
    if (line.length === 0) {
      continue;
    }

    for (const normalizedWord of tokenizeLine(line)) {
      const row = {
        language: source.language,
        word: line,
        normalizedWord,
        source: `bundled:${source.csv}`
      };

      if (!seen.has(normalizedWord)) {
        rows.push(normalizedWord);
      }
      seen.set(normalizedWord, row);
    }
  }

  return rows.map((normalizedWord) => seen.get(normalizedWord));
}

function renderInsert(rows) {
  const statements = [];
  for (const rowBatch of chunk(rows, 250)) {
    const values = rowBatch
      .map(
        (row) =>
          `  ('${escapeSqlLiteral(row.language)}', '${escapeSqlLiteral(row.word)}', '${escapeSqlLiteral(row.normalizedWord)}', '${escapeSqlLiteral(row.source)}')`
      )
      .join(",\n");

    statements.push(
      `INSERT OR REPLACE INTO bm25_stopwords (language, word, normalized_word, source)\nVALUES\n${values};`
    );
  }

  return statements.join("\n\n");
}

async function main() {
  const parts = [
    "-- Bundled BM25 stopword seed data for SQLite bootstraps.",
    "-- Generated from sql/stopwords/data/*.csv. Safe to run repeatedly.",
    "-- The schema creates bm25_stopwords; this file only refreshes the bundled rows.",
    "",
    "BEGIN;",
    "",
    "DELETE FROM bm25_stopwords WHERE source LIKE 'bundled:%';",
    ""
  ];

  for (const source of stopwordSources) {
    const rows = await loadRowsForSource(source);
    if (rows.length === 0) {
      continue;
    }

    parts.push(`-- ${source.csv}`);
    parts.push(renderInsert(rows));
    parts.push("");
  }

  parts.push("COMMIT;");
  parts.push("");

  const output = parts.join("\n");
  await Promise.all(outputPaths.map((outputPath) => writeFile(outputPath, output)));
}

await main();
