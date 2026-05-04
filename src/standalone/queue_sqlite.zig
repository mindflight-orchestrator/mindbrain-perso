const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const compat = @import("zig16_compat.zig");

pub const Database = facet_sqlite.Database;
pub const c = facet_sqlite.c;
const Error = facet_sqlite.Error;

fn unixTimestamp() i64 {
    return std.Io.Timestamp.now(compat.io(), .real).toSeconds();
}

fn milliTimestamp() i64 {
    return std.Io.Timestamp.now(compat.io(), .real).toMilliseconds();
}

pub const Message = struct {
    msg_id: i64,
    read_ct: i64,
    enqueued_at_unix: i64,
    vt_until_unix: ?i64,
    message: []const u8,
};

pub const QueueStore = struct {
    db: Database,
    allocator: std.mem.Allocator,

    pub fn init(db: Database, allocator: std.mem.Allocator) QueueStore {
        return .{
            .db = db,
            .allocator = allocator,
        };
    }

    pub fn sendText(self: *QueueStore, queue_name: []const u8, message: []const u8) !i64 {
        try ensureQueue(self.db, queue_name);

        const stmt = try prepare(self.db, "INSERT INTO queue_messages(queue_name, read_ct, enqueued_at_unix, vt_until_unix, archived, archived_at_unix, deleted, deleted_at_unix, message) VALUES (?1, 0, ?2, NULL, 0, NULL, 0, NULL, ?3)");
        defer finalize(stmt);

        try bindText(stmt, 1, queue_name);
        try bindInt64(stmt, 2, unixTimestamp());
        try bindText(stmt, 3, message);
        try stepDone(stmt);
        return c.sqlite3_last_insert_rowid(self.db.handle);
    }

    pub fn sendJson(self: *QueueStore, queue_name: []const u8, payload: anytype) !i64 {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();
        try out.writer.print("{f}", .{std.json.fmt(payload, .{})});
        const json = try out.toOwnedSlice();
        defer self.allocator.free(json);
        return try self.sendText(queue_name, json);
    }

    pub fn read(self: *QueueStore, queue_name: []const u8, visibility_timeout_seconds: i64, limit_n: usize) ![]Message {
        if (limit_n == 0) return try self.allocator.alloc(Message, 0);

        try beginImmediate(self.db);
        errdefer rollback(self.db);

        const now = unixTimestamp();
        const lease_until = now + @max(visibility_timeout_seconds, 0);
        const select_stmt = try prepare(
            self.db,
            "SELECT msg_id, read_ct, enqueued_at_unix, message FROM queue_messages WHERE queue_name = ?1 AND archived = 0 AND deleted = 0 AND (vt_until_unix IS NULL OR vt_until_unix <= ?2) ORDER BY msg_id ASC LIMIT ?3",
        );
        defer finalize(select_stmt);
        try bindText(select_stmt, 1, queue_name);
        try bindInt64(select_stmt, 2, now);
        try bindInt64(select_stmt, 3, limit_n);

        const update_stmt = try prepare(
            self.db,
            "UPDATE queue_messages SET read_ct = read_ct + 1, vt_until_unix = ?2 WHERE msg_id = ?1 AND queue_name = ?3 AND archived = 0 AND deleted = 0",
        );
        defer finalize(update_stmt);

        var out = std.ArrayList(Message).empty;
        errdefer {
            for (out.items) |row| self.allocator.free(row.message);
            out.deinit(self.allocator);
        }

        while (true) {
            const rc = c.sqlite3_step(select_stmt);
            if (rc == c.SQLITE_DONE) break;
            if (rc != c.SQLITE_ROW) return error.StepFailed;

            const msg_id = try columnI64(select_stmt, 0);
            const read_ct = try columnI64(select_stmt, 1);
            const enqueued_at = try columnI64(select_stmt, 2);
            const message = try dupeColumnText(self.allocator, select_stmt, 3);

            try resetStatement(update_stmt);
            try bindInt64(update_stmt, 1, msg_id);
            try bindInt64(update_stmt, 2, lease_until);
            try bindText(update_stmt, 3, queue_name);
            try stepDone(update_stmt);

            try out.append(self.allocator, .{
                .msg_id = msg_id,
                .read_ct = read_ct + 1,
                .enqueued_at_unix = enqueued_at,
                .vt_until_unix = lease_until,
                .message = message,
            });
        }

        try resetStatement(select_stmt);
        try commit(self.db);
        return out.toOwnedSlice(self.allocator);
    }

    pub fn archive(self: *QueueStore, queue_name: []const u8, msg_id: i64) !bool {
        try ensureQueue(self.db, queue_name);

        const stmt = try prepare(
            self.db,
            "UPDATE queue_messages SET archived = 1, archived_at_unix = ?3, vt_until_unix = NULL WHERE queue_name = ?1 AND msg_id = ?2 AND archived = 0 AND deleted = 0",
        );
        defer finalize(stmt);

        try bindText(stmt, 1, queue_name);
        try bindInt64(stmt, 2, msg_id);
        try bindInt64(stmt, 3, unixTimestamp());
        try stepDone(stmt);
        return c.sqlite3_changes(self.db.handle) > 0;
    }

    pub fn delete(self: *QueueStore, queue_name: []const u8, msg_id: i64) !bool {
        try ensureQueue(self.db, queue_name);

        const stmt = try prepare(
            self.db,
            "UPDATE queue_messages SET deleted = 1, deleted_at_unix = ?3, vt_until_unix = NULL WHERE queue_name = ?1 AND msg_id = ?2 AND deleted = 0",
        );
        defer finalize(stmt);

        try bindText(stmt, 1, queue_name);
        try bindInt64(stmt, 2, msg_id);
        try bindInt64(stmt, 3, unixTimestamp());
        try stepDone(stmt);
        return c.sqlite3_changes(self.db.handle) > 0;
    }

    pub fn pollLoop(
        self: *QueueStore,
        queue_name: []const u8,
        visibility_timeout_seconds: i64,
        batch_size: usize,
        handler: *const fn (std.mem.Allocator, Message) anyerror!void,
    ) !void {
        const min_sleep_ns: u64 = 50 * std.time.ns_per_ms;
        const max_sleep_ns: u64 = 2 * std.time.ns_per_s;
        var sleep_ns = min_sleep_ns;

        while (true) {
            const msgs = self.read(queue_name, visibility_timeout_seconds, batch_size) catch |err| {
                std.log.warn("queue {s} read failed: {s}", .{ queue_name, @errorName(err) });
                std.Thread.sleep(max_sleep_ns);
                continue;
            };
            defer {
                for (msgs) |msg| self.allocator.free(msg.message);
                self.allocator.free(msgs);
            }

            if (msgs.len == 0) {
                std.Thread.sleep(sleep_ns);
                sleep_ns = @min(sleep_ns * 2, max_sleep_ns);
                continue;
            }

            sleep_ns = min_sleep_ns;

            for (msgs) |msg| {
                handler(self.allocator, msg) catch |err| {
                    std.log.warn("queue {s} handler failed: {s}", .{ queue_name, @errorName(err) });
                    continue;
                };
                _ = try self.archive(queue_name, msg.msg_id);
            }
        }
    }
};

pub fn ensureQueue(db: Database, queue_name: []const u8) !void {
    const stmt = try prepare(db, "INSERT OR IGNORE INTO queue_registry(queue_name) VALUES (?1)");
    defer finalize(stmt);
    try bindText(stmt, 1, queue_name);
    try stepDone(stmt);
}

fn beginImmediate(db: Database) !void {
    try db.exec("BEGIN IMMEDIATE");
}

fn commit(db: Database) !void {
    try db.exec("COMMIT");
}

fn rollback(db: Database) void {
    _ = db.exec("ROLLBACK") catch {};
}

fn prepare(db: Database, sql: []const u8) !*c.sqlite3_stmt {
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

fn finalize(stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
}

fn resetStatement(stmt: *c.sqlite3_stmt) !void {
    if (c.sqlite3_reset(stmt) != c.SQLITE_OK) return error.StepFailed;
    if (c.sqlite3_clear_bindings(stmt) != c.SQLITE_OK) return error.BindFailed;
}

fn stepDone(stmt: *c.sqlite3_stmt) !void {
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.StepFailed;
}

fn bindInt64(stmt: *c.sqlite3_stmt, index: c_int, value: anytype) !void {
    if (c.sqlite3_bind_int64(stmt, index, @intCast(value)) != c.SQLITE_OK) return error.BindFailed;
}

fn bindText(stmt: *c.sqlite3_stmt, index: c_int, value: []const u8) !void {
    if (c.sqlite3_bind_text(stmt, index, value.ptr, @intCast(value.len), facet_sqlite.sqliteTransient()) != c.SQLITE_OK) {
        return error.BindFailed;
    }
}

fn columnI64(stmt: *c.sqlite3_stmt, index: c_int) !i64 {
    return c.sqlite3_column_int64(stmt, index);
}

fn dupeColumnText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, index: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, index);
    const ptr = c.sqlite3_column_text(stmt, index) orelse return error.MissingRow;
    const bytes: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
    return allocator.dupe(u8, bytes);
}

test "queue sqlite sends, reads, archives, and deletes" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var queue = QueueStore.init(db, std.testing.allocator);
    const msg_id = try queue.sendText("signal_raw", "{\"hello\":\"world\"}");
    try std.testing.expect(msg_id > 0);

    const msgs = try queue.read("signal_raw", 30, 10);
    defer {
        for (msgs) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(msgs);
    }
    try std.testing.expectEqual(@as(usize, 1), msgs.len);
    try std.testing.expectEqualStrings("{\"hello\":\"world\"}", msgs[0].message);
    try std.testing.expectEqual(@as(i64, 1), msgs[0].read_ct);

    try std.testing.expect(try queue.archive("signal_raw", msg_id));
    const empty = try queue.read("signal_raw", 30, 10);
    defer {
        for (empty) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(empty);
    }
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    const msg_id_2 = try queue.sendText("signal_raw", "{\"hello\":\"again\"}");
    try std.testing.expect(msg_id_2 > 0);
    try std.testing.expect(try queue.delete("signal_raw", msg_id_2));
}

test "queue sqlite respects visibility timeout" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var queue = QueueStore.init(db, std.testing.allocator);
    const msg_id = try queue.sendText("signal_raw", "{\"step\":1}");
    try std.testing.expect(msg_id > 0);

    const first = try queue.read("signal_raw", 60, 1);
    defer {
        for (first) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(first);
    }
    try std.testing.expectEqual(@as(usize, 1), first.len);

    const second = try queue.read("signal_raw", 60, 1);
    defer {
        for (second) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(second);
    }
    try std.testing.expectEqual(@as(usize, 0), second.len);
}

test "queue sqlite reclaims a message after immediate lease expiry" {
    var db = try Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    var queue = QueueStore.init(db, std.testing.allocator);
    const msg_id = try queue.sendText("signal_raw", "{\"step\":2}");
    try std.testing.expect(msg_id > 0);

    const first = try queue.read("signal_raw", 0, 1);
    defer {
        for (first) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(first);
    }
    try std.testing.expectEqual(@as(usize, 1), first.len);
    try std.testing.expectEqual(@as(i64, 1), first[0].read_ct);

    const second = try queue.read("signal_raw", 0, 1);
    defer {
        for (second) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(second);
    }
    try std.testing.expectEqual(@as(usize, 1), second.len);
    try std.testing.expectEqualStrings(first[0].message, second[0].message);
    try std.testing.expectEqual(@as(i64, 2), second[0].read_ct);
}

test "queue sqlite preserves leased messages across reopen" {
    var path_buf: [128]u8 = undefined;
    const db_path = try std.fmt.bufPrint(&path_buf, "/tmp/mindbrain-queue-{d}.sqlite", .{milliTimestamp()});

    var db = try Database.open(db_path);
    try db.applyStandaloneSchema();

    var queue = QueueStore.init(db, std.testing.allocator);
    const msg_id = try queue.sendText("signal_raw", "{\"step\":3}");
    try std.testing.expect(msg_id > 0);

    const leased = try queue.read("signal_raw", 60, 1);
    defer {
        for (leased) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(leased);
    }
    try std.testing.expectEqual(@as(usize, 1), leased.len);
    try std.testing.expectEqual(@as(i64, 1), leased[0].read_ct);

    db.close();

    db = try Database.open(db_path);
    defer db.close();
    try db.applyStandaloneSchema();

    var reopened_queue = QueueStore.init(db, std.testing.allocator);
    const still_leased = try reopened_queue.read("signal_raw", 60, 1);
    defer {
        for (still_leased) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(still_leased);
    }
    try std.testing.expectEqual(@as(usize, 0), still_leased.len);

    var sql_buf: [160]u8 = undefined;
    const expire_sql = try std.fmt.bufPrint(
        &sql_buf,
        "UPDATE queue_messages SET vt_until_unix = 0 WHERE queue_name = 'signal_raw' AND msg_id = {d}",
        .{msg_id},
    );
    try db.exec(expire_sql);

    const reclaimed = try reopened_queue.read("signal_raw", 60, 1);
    defer {
        for (reclaimed) |msg| std.testing.allocator.free(msg.message);
        std.testing.allocator.free(reclaimed);
    }
    try std.testing.expectEqual(@as(usize, 1), reclaimed.len);
    try std.testing.expectEqualStrings("{\"step\":3}", reclaimed[0].message);
    try std.testing.expectEqual(@as(i64, 2), reclaimed[0].read_ct);
}
