#!/usr/bin/env node
/**
 * Seed immeuble-demo projections into the local SQLite file.
 * Idempotent: skips rows that match agent_id + scope + proj_type + content.
 */
import { randomUUID } from 'node:crypto';
import { readFileSync, existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const STUDIO_ROOT = join(SCRIPT_DIR, '..');
const GHOSTCRAB_ROOT = process.env.GHOSTCRAB_ROOT ?? join(STUDIO_ROOT, '..', 'ghostcrab-personal-mcp');
const SQLITE_PATH = process.env.GHOSTCRAB_SQLITE_PATH ?? join(STUDIO_ROOT, 'data', 'immeuble-demo.sqlite');
const AGENT_ID = process.env.GHOSTCRAB_AGENT_ID ?? 'agent:self';
const SEED_FILE =
	process.env.IMMEUBLE_PROJECTIONS_SEED ??
	join(GHOSTCRAB_ROOT, 'examples', 'immeuble-demo', 'projections.seed.jsonl');

function runSqlite(sql) {
	const result = spawnSync('sqlite3', [SQLITE_PATH, sql], { encoding: 'utf8' });
	if (result.status !== 0) {
		throw new Error(result.stderr || result.stdout || 'sqlite3 failed');
	}
	return result.stdout.trim();
}

function escapeSql(value) {
	return `'${String(value).replace(/'/g, "''")}'`;
}

function main() {
	if (!existsSync(SQLITE_PATH)) {
		console.error(`error: SQLite file not found: ${SQLITE_PATH}`);
		console.error('Run pnpm load:immeuble first.');
		process.exit(1);
	}
	if (!existsSync(SEED_FILE)) {
		console.error(`error: seed file not found: ${SEED_FILE}`);
		process.exit(1);
	}
	if (spawnSync('sqlite3', ['-version']).status !== 0) {
		console.error('error: sqlite3 CLI is required');
		process.exit(1);
	}

	const lines = readFileSync(SEED_FILE, 'utf8')
		.split('\n')
		.map((line) => line.trim())
		.filter(Boolean);

	let inserted = 0;
	let updated = 0;
	let skipped = 0;
	const now = Math.floor(Date.now() / 1000);

	for (const line of lines) {
		const row = JSON.parse(line);
		const scope = String(row.scope ?? 'immeuble-demo');
		const projType = String(row.proj_type ?? 'STEP');
		const content = String(row.content ?? '');
		const weight = Number(row.weight ?? 0.7);
		const status = String(row.status ?? 'active');
		const sourceRef = row.source_ref == null ? null : String(row.source_ref);
		const sourceType = 'seed:immeuble-demo';

		if (!content) {
			skipped += 1;
			continue;
		}

		const existing = runSqlite(
			`SELECT id FROM projections WHERE agent_id=${escapeSql(AGENT_ID)} AND scope=${escapeSql(scope)} AND proj_type=${escapeSql(projType)} AND content=${escapeSql(content)} LIMIT 1;`
		);

		if (existing) {
			runSqlite(
				`UPDATE projections SET weight=${weight}, status=${escapeSql(status)}, source_type=${escapeSql(sourceType)}, source_ref=${sourceRef ? escapeSql(sourceRef) : 'NULL'} WHERE id=${escapeSql(existing)};`
			);
			updated += 1;
			continue;
		}

		const id = randomUUID();
		runSqlite(
			`INSERT INTO projections (id, agent_id, scope, proj_type, content, weight, source_ref, source_type, status, created_at_unix) VALUES (${escapeSql(id)}, ${escapeSql(AGENT_ID)}, ${escapeSql(scope)}, ${escapeSql(projType)}, ${escapeSql(content)}, ${weight}, ${sourceRef ? escapeSql(sourceRef) : 'NULL'}, ${escapeSql(sourceType)}, ${escapeSql(status)}, ${now});`
		);
		inserted += 1;
	}

	const count = runSqlite(
		`SELECT COUNT(*) FROM projections WHERE agent_id=${escapeSql(AGENT_ID)} AND scope='immeuble-demo';`
	);
	console.log(`==> Projections seeded into ${SQLITE_PATH}`);
	console.log(`    inserted=${inserted} updated=${updated} skipped=${skipped} total=${count}`);
}

main();
