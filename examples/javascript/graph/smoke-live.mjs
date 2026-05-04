import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

function run(cmd, args, options = {}) {
  execFileSync(cmd, args, {
    stdio: "inherit",
    env: {
      ...process.env,
      ZIG_LOCAL_CACHE_DIR: process.env.ZIG_LOCAL_CACHE_DIR ?? path.join(os.tmpdir(), "zig-cache"),
      ZIG_GLOBAL_CACHE_DIR: process.env.ZIG_GLOBAL_CACHE_DIR ?? path.join(os.tmpdir(), "zig-global-cache"),
    },
    ...options,
  });
}

async function waitForHealth(url, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) {
        return;
      }
    } catch {
      // Keep retrying until the server is up.
    }
    await delay(250);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function readSse(url) {
  const res = await fetch(url, {
    headers: {
      accept: "text/event-stream",
    },
  });
  assert.equal(res.ok, true, `expected SSE response, got ${res.status}`);

  const text = await res.text();
  const events = [];
  for (const block of text.trim().split(/\n\s*\n/)) {
    let kind = null;
    let id = null;
    let data = "";
    for (const line of block.split("\n")) {
      if (line.startsWith("event: ")) {
        kind = line.slice("event: ".length).trim();
      } else if (line.startsWith("id: ")) {
        id = Number(line.slice("id: ".length).trim());
      } else if (line.startsWith("data: ")) {
        data += line.slice("data: ".length);
      }
    }
    if (kind && data) {
      events.push({
        kind,
        id,
        data: JSON.parse(data),
      });
    }
  }
  return { text, events };
}

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const dbDir = fs.mkdtempSync(path.join(os.tmpdir(), "mindbrain-graph-live-"));
const dbPath = path.join(dbDir, "graph.sqlite");
const port = 18091;
let server = null;

async function main() {
  run("zig", ["build", "standalone-tool"], { cwd: repoRoot });
  run("zig", ["build", "standalone-http"], { cwd: repoRoot });

  run(path.join(repoRoot, "zig-out/bin/mindbrain-standalone-tool"), [
    "seed-demo",
    "--db",
    dbPath,
  ], { cwd: repoRoot });

  const addr = `127.0.0.1:${port}`;
  const serverPath = path.join(repoRoot, "zig-out/bin/mindbrain-http");
  server = spawn(serverPath, ["--addr", addr, "--db", dbPath], {
    cwd: repoRoot,
    stdio: ["ignore", "pipe", "pipe"],
  });

  server.stdout.on("data", (chunk) => process.stdout.write(chunk));
  server.stderr.on("data", (chunk) => process.stderr.write(chunk));

  try {
    await waitForHealth(`http://${addr}/health`);
    const sse = await readSse(`http://${addr}/api/mindbrain/graph/subgraph?seed_ids=1&hops=2`);

    assert.ok(sse.text.includes("event: seed_node"));
    assert.ok(sse.text.includes("event: edge"));
    assert.ok(sse.text.includes("event: node"));
    assert.ok(sse.text.includes("event: done"));

    assert.deepEqual(
      sse.events.map((event) => event.kind),
      ["seed_node", "edge", "node", "done"],
    );
    assert.equal(sse.events[0].data.entity.entity_id, 1);
    assert.equal(sse.events[1].data.direction, "outbound");
    assert.equal(sse.events[1].data.relation.relation_type, "works_for");
    assert.equal(sse.events[2].data.entity.entity_id, 2);
    assert.equal(sse.events[3].data.kind, "subgraph");
    assert.equal(sse.events[3].data.seed_count, 1);
    assert.ok(sse.events[3].data.node_count >= 2);
    assert.ok(sse.events[3].data.edge_count >= 1);
  } finally {
    if (server) {
      server.kill("SIGTERM");
      let exited = false;
      const exitPromise = new Promise((resolve) => {
        server.once("exit", () => {
          exited = true;
          resolve();
        });
      });
      await Promise.race([exitPromise, delay(5_000)]);
      if (!exited) {
        server.kill("SIGKILL");
        await exitPromise.catch(() => {});
      }
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
}).finally(() => {
  try {
    fs.rmSync(dbDir, { recursive: true, force: true });
  } catch {
    // Ignore cleanup failures.
  }
});
