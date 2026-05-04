const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const search_sqlite = @import("search_sqlite.zig");
const compatibility_sqlite = @import("compatibility_sqlite.zig");
const tokenization_sqlite = @import("tokenization_sqlite.zig");
const workspace_sqlite = @import("workspace_sqlite.zig");

test "native tokenization compatibility exposes pure word splitting and hashing" {
    var tokens = try tokenization_sqlite.tokenizePure("Hello, THE world! SQLite rocks.", std.testing.allocator);
    defer {
        for (tokens.items) |token| std.testing.allocator.free(token);
        tokens.deinit(std.testing.allocator);
    }

    try std.testing.expectEqual(@as(usize, 4), tokens.items.len);
    try std.testing.expectEqualStrings("hello", tokens.items[0]);
    try std.testing.expectEqualStrings("world", tokens.items[1]);
    try std.testing.expectEqualStrings("sqlite", tokens.items[2]);
    try std.testing.expectEqualStrings("rocks", tokens.items[3]);

    var hashes = try tokenization_sqlite.tokenizeToHashes("hello world sqlite", std.testing.allocator);
    defer hashes.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), hashes.items.len);
    try std.testing.expectEqual(tokenization_sqlite.hashLexeme("hello"), hashes.items[0]);
    try std.testing.expectEqual(tokenization_sqlite.hashLexeme("world"), hashes.items[1]);
    try std.testing.expectEqual(tokenization_sqlite.hashLexeme("sqlite"), hashes.items[2]);
}

test "native tokenization compatibility handles large repeated input without truncation" {
    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(std.testing.allocator);
    try buffer.ensureTotalCapacity(std.testing.allocator, 4096);
    var i: usize = 0;
    while (i < 256) : (i += 1) {
        try buffer.appendSlice(std.testing.allocator, "Standalone SQLite full text search with compatibility. ");
    }

    var tokens = try tokenization_sqlite.tokenizePure(buffer.items, std.testing.allocator);
    defer {
        for (tokens.items) |token| std.testing.allocator.free(token);
        tokens.deinit(std.testing.allocator);
    }

    try std.testing.expect(tokens.items.len > 0);
    try std.testing.expect(tokens.items.len >= 1024);
}

test "compatibility report surfaces supported legacy versions and unsupported DDL trigger semantics" {
    const versions = compatibility_sqlite.supportedUpstreamVersions();
    try std.testing.expectEqual(@as(usize, 3), versions.len);
    try std.testing.expectEqualStrings("0.3.9", versions[0]);
    try std.testing.expectEqualStrings("0.4.1", versions[1]);
    try std.testing.expectEqualStrings("0.4.3", versions[2]);

    const flags = compatibility_sqlite.compatibilityFlags();
    try std.testing.expect(flags.native_tokenization);
    try std.testing.expect(flags.bm25_sync_hooks);
    try std.testing.expect(!flags.ddl_triggers_supported);
    try std.testing.expect(!flags.parallel_indexing);
    try std.testing.expect(!flags.unlogged_tables);
}

test "BM25 helper compatibility keeps table semantics and sync hooks wired" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try workspace_sqlite.upsertWorkspace(db, "default", "{}");
    try search_sqlite.setupSearchTable(db, std.testing.allocator, .{
        .table_id = 41,
        .workspace_id = "default",
        .schema_name = "public",
        .table_name = "articles",
        .key_column = "id",
        .content_column = "content",
        .metadata_column = "metadata",
        .language = "english",
        .populate = false,
    });

    const toon = try workspace_sqlite.exportWorkspaceModelToon(db, std.testing.allocator, "default");
    defer std.testing.allocator.free(toon);
    try std.testing.expect(std.mem.indexOf(u8, toon, "articles") != null);

    try search_sqlite.syncSearchDocument(db, std.testing.allocator, 41, 7, "native tokenization and search", "english");
    try search_sqlite.syncSearchDocument(db, std.testing.allocator, 41, 8, "native tokenization and filters", "english");

    var store = try search_sqlite.loadSearchStore(db, std.testing.allocator);
    defer store.deinit();

    const repo = store.bm25Repository();
    var bitmap = (try repo.getPostingBitmapFn(repo.ctx, std.testing.allocator, 41, tokenization_sqlite.hashLexeme("native"))).?;
    defer bitmap.deinit();
    try std.testing.expect(bitmap.contains(7));
    try std.testing.expect(bitmap.contains(8));

    try search_sqlite.syncDeleteSearchDocument(db, std.testing.allocator, 41, 7);
    try search_sqlite.syncDeleteSearchDocument(db, std.testing.allocator, 41, 8);
    const snapshot = try search_sqlite.compactSearchSnapshot(db, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.postings);
}

test "BM25 sync registration create and drop updates the registry" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try workspace_sqlite.upsertWorkspace(db, "default", "{}");
    try search_sqlite.setupSearchTable(db, std.testing.allocator, .{
        .table_id = 42,
        .workspace_id = "default",
        .schema_name = "public",
        .table_name = "articles",
        .key_column = "id",
        .content_column = "content",
        .metadata_column = "metadata",
        .language = "english",
        .populate = false,
    });

    try std.testing.expect(!try search_sqlite.hasBm25SyncTrigger(db, 42));
    try search_sqlite.bm25CreateSyncTrigger(db, .{
        .table_id = 42,
        .id_column = "id",
        .content_column = "content",
        .language = "english",
    });
    try std.testing.expect(try search_sqlite.hasBm25SyncTrigger(db, 42));

    try search_sqlite.syncSearchDocumentIfTriggered(db, std.testing.allocator, 42, 7, "triggered bm25 indexing", "english");
    var store = try search_sqlite.loadSearchStore(db, std.testing.allocator);
    defer store.deinit();
    const repo = store.bm25Repository();
    var bitmap = (try repo.getPostingBitmapFn(repo.ctx, std.testing.allocator, 42, tokenization_sqlite.hashLexeme("triggered"))).?;
    defer bitmap.deinit();
    try std.testing.expect(bitmap.contains(7));

    try search_sqlite.bm25DropSyncTrigger(db, 42);
    try std.testing.expect(!try search_sqlite.hasBm25SyncTrigger(db, 42));
    try search_sqlite.syncSearchDocumentIfTriggered(db, std.testing.allocator, 42, 8, "dropped trigger should not sync", "english");

    var reloaded = try search_sqlite.loadSearchStore(db, std.testing.allocator);
    defer reloaded.deinit();
    const reloaded_repo = reloaded.bm25Repository();
    try std.testing.expect((try reloaded_repo.getPostingBitmapFn(reloaded_repo.ctx, std.testing.allocator, 42, tokenization_sqlite.hashLexeme("dropped"))) == null);
}
