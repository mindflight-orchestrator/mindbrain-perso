# SQL session open lock double-close

## Context

GhostCrab integration runs exposed a MindBrain standalone HTTP crash when two SQL sessions contend for the same SQLite database. The first session opens successfully and holds a `BEGIN IMMEDIATE` write transaction. A second `POST /api/mindbrain/sql/session/open` then waits for `PRAGMA busy_timeout` and fails with `database is locked`.

Before the fix, the error path in `src/standalone/http_server.zig` explicitly called `session.db.close()` and then returned the error while `errdefer session.db.close()` was still armed. In debug builds, `Database.close()` poisons the handle after closing, so the deferred second close reached SQLite with a poisoned pointer such as `0xaaaaaaaaaaaaaaaa` and panicked in `sqlite3_close_v2`.

## Fix

Let the existing `errdefer session.db.close()` own cleanup when `BEGIN IMMEDIATE` fails:

```zig
try session.db.exec("BEGIN IMMEDIATE");
```

This preserves the intended cleanup while avoiding a double-close. The second session should now return a structured HTTP error for the lock condition, and the backend process should remain alive.

## Reproduction

1. Start the standalone HTTP server against a file-backed SQLite database.
2. Open a SQL session with `POST /api/mindbrain/sql/session/open`.
3. Without closing the first session, send a second `POST /api/mindbrain/sql/session/open`.
4. Observe that the second request fails after the busy timeout.

Expected behavior after the fix:

- the second request returns an HTTP error instead of crashing the process;
- the first session can still be closed with `commit=false`;
- `GET /health` still returns `ok`.

## Tests to Add

Add an integration or standalone HTTP regression test that:

1. Starts the HTTP app on a temporary SQLite file.
2. Opens one SQL session and keeps it open.
3. Attempts to open a second SQL session.
4. Asserts the second open fails without process panic.
5. Closes the first session with rollback.
6. Asserts the server remains healthy.

If a full process-level HTTP test is too heavy, add a narrower handler-level test that exercises `handleSqlSessionOpen` twice against the same DB path and verifies that the failed second open does not double-close the SQLite handle.

## Downstream Sync

After this lands in the master MindBrain repo, update vendored copies such as `ghostcrab-personal-mcp/vendor/mindbrain` from the master commit instead of patching the vendor tree directly.
