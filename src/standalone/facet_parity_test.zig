const std = @import("std");
const facet_sqlite = @import("facet_sqlite.zig");
const query_executor = @import("query_executor.zig");
const roaring = @import("roaring.zig");

test "facet parity setup exposes table metadata and facet names" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        101,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
            .{ .facet_id = 2, .facet_name = "region" },
        },
    );

    const names = try facet_sqlite.listTableFacetNames(db, std.testing.allocator, "documents");
    defer {
        for (names) |name| std.testing.allocator.free(name);
        std.testing.allocator.free(names);
    }

    try std.testing.expectEqual(@as(usize, 2), names.len);
    try std.testing.expectEqualStrings("category", names[0]);
    try std.testing.expectEqualStrings("region", names[1]);

    const summary = try facet_sqlite.describeFacetTable(db, std.testing.allocator, "documents");
    defer {
        std.testing.allocator.free(summary.schema_name);
        std.testing.allocator.free(summary.table_name);
    }

    try std.testing.expectEqual(@as(u64, 101), summary.table_id);
    try std.testing.expectEqual(@as(u8, 4), summary.chunk_bits);
    try std.testing.expectEqual(@as(u64, 2), summary.facet_count);
    try std.testing.expectEqual(@as(u64, 0), summary.posting_count);
    try std.testing.expectEqual(@as(u64, 0), summary.value_node_count);
}

test "facet parity minimal setup supports filtering and facet counts" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        1,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
            .{ .facet_id = 2, .facet_name = "region" },
        },
    );

    var category_technology = try roaring.Bitmap.fromSlice(&.{ 1, 2, 4 });
    defer category_technology.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 1, 1, "Technology", 0, category_technology);

    var category_cooking = try roaring.Bitmap.fromSlice(&.{3});
    defer category_cooking.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 1, 1, "Cooking", 0, category_cooking);

    var region_eu = try roaring.Bitmap.fromSlice(&.{ 1, 4 });
    defer region_eu.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 1, 2, "EU", 0, region_eu);

    var region_us = try roaring.Bitmap.fromSlice(&.{2});
    defer region_us.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 1, 2, "US", 0, region_us);

    var runtime = try query_executor.Runtime.init(&db, std.testing.allocator);
    defer runtime.deinit();

    var result = try runtime.execute(std.testing.allocator, .{
        .workspace_id = "default",
        .table_name = "documents",
        .facet_filters = &.{
            .{ .facet_name = "category", .values = &.{"Technology"} },
        },
        .count_facets = &.{
            "category",
            "region",
        },
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.documents != null);
    const docs = try result.documents.?.toArray(std.testing.allocator);
    defer std.testing.allocator.free(docs);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 4 }, docs);

    try std.testing.expectEqual(@as(usize, 2), result.facet_counts.len);
    try std.testing.expectEqualStrings("category", result.facet_counts[0].facet_name);
    try std.testing.expectEqual(@as(usize, 1), result.facet_counts[0].counts.len);
    try std.testing.expectEqualStrings("Technology", result.facet_counts[0].counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 3), result.facet_counts[0].counts[0].cardinality);

    try std.testing.expectEqualStrings("region", result.facet_counts[1].facet_name);
    try std.testing.expectEqual(@as(usize, 2), result.facet_counts[1].counts.len);
    try std.testing.expectEqualStrings("EU", result.facet_counts[1].counts[0].facet_value);
    try std.testing.expectEqual(@as(u64, 2), result.facet_counts[1].counts[0].cardinality);
    try std.testing.expectEqualStrings("US", result.facet_counts[1].counts[1].facet_value);
    try std.testing.expectEqual(@as(u64, 1), result.facet_counts[1].counts[1].cardinality);
}

test "facet parity regression keeps facet groups populated under filters" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        2,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
            .{ .facet_id = 2, .facet_name = "subcategory" },
        },
    );

    var category_technology = try roaring.Bitmap.fromSlice(&.{ 1, 2, 3, 6, 7 });
    defer category_technology.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 2, 1, "Technology", 0, category_technology);

    var category_cooking = try roaring.Bitmap.fromSlice(&.{ 4, 8 });
    defer category_cooking.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 2, 1, "Cooking", 0, category_cooking);

    var subcategory_database = try roaring.Bitmap.fromSlice(&.{ 1, 6 });
    defer subcategory_database.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 2, 2, "Database", 0, subcategory_database);

    var subcategory_programming = try roaring.Bitmap.fromSlice(&.{ 2, 3, 7 });
    defer subcategory_programming.deinit();
    try facet_sqlite.insertPostingBitmap(db, std.testing.allocator, 2, 2, "Programming", 0, subcategory_programming);

    var runtime = try query_executor.Runtime.init(&db, std.testing.allocator);
    defer runtime.deinit();

    var result = try runtime.execute(std.testing.allocator, .{
        .workspace_id = "default",
        .table_name = "documents",
        .facet_filters = &.{
            .{ .facet_name = "category", .values = &.{"Technology"} },
        },
        .count_facets = &.{
            "category",
            "subcategory",
        },
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.documents != null);
    try std.testing.expect(result.facet_counts.len > 0);
    try std.testing.expect(result.facet_counts[0].counts.len > 0);
    try std.testing.expect(result.facet_counts[1].counts.len > 0);
}

test "facet delta merge applies queued deltas and clears the queue" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        20,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
        },
    );

    var initial = try roaring.Bitmap.fromSlice(&.{ 1, 2 });
    defer initial.deinit();
    try facet_sqlite.upsertPostingBitmap(db, std.testing.allocator, 20, 1, "technology", 0, initial);

    try facet_sqlite.queueFacetDelta(db, 20, 1, "technology", 3, 1);
    try facet_sqlite.queueFacetDelta(db, 20, 1, "technology", 1, -1);

    try std.testing.expectEqual(@as(u64, 2), try facet_sqlite.countFacetDeltas(db, 20, null));
    try std.testing.expectEqual(@as(u64, 1), try facet_sqlite.mergeDeltasSafe(db, 20, null));
    try std.testing.expectEqual(@as(u64, 0), try facet_sqlite.countFacetDeltas(db, 20, null));

    var restored = try facet_sqlite.loadPostingBitmap(db, 20, 1, "technology", 0);
    defer restored.deinit();
    const ids = try restored.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);

    try std.testing.expectEqualSlices(u32, &.{ 2, 3 }, ids);
}

test "facet delta merge rolls back atomically on invalid posting data" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        21,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
        },
    );

    var good = try roaring.Bitmap.fromSlice(&.{1});
    defer good.deinit();
    try facet_sqlite.upsertPostingBitmap(db, std.testing.allocator, 21, 1, "good", 0, good);
    try db.exec("INSERT INTO facet_postings(table_id, facet_id, facet_value, chunk_id, posting_blob) VALUES (21, 1, 'bad', 0, x'00')");

    try facet_sqlite.queueFacetDelta(db, 21, 1, "good", 2, 1);
    try facet_sqlite.queueFacetDelta(db, 21, 1, "bad", 3, 1);

    try std.testing.expectError(error.DeserializeFailed, facet_sqlite.mergeDeltasSafe(db, 21, null));
    try std.testing.expectEqual(@as(u64, 2), try facet_sqlite.countFacetDeltas(db, 21, null));

    var restored = try facet_sqlite.loadPostingBitmap(db, 21, 1, "good", 0);
    defer restored.deinit();
    const ids = try restored.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);
    try std.testing.expectEqualSlices(u32, &.{1}, ids);
}

test "facet delta merge nests cleanly inside an outer savepoint" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        22,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
        },
    );

    var initial = try roaring.Bitmap.fromSlice(&.{1});
    defer initial.deinit();
    try facet_sqlite.upsertPostingBitmap(db, std.testing.allocator, 22, 1, "technology", 0, initial);

    try db.exec("SAVEPOINT outer_facet_tx");
    defer {
        _ = db.exec("ROLLBACK TO SAVEPOINT outer_facet_tx") catch {};
        _ = db.exec("RELEASE SAVEPOINT outer_facet_tx") catch {};
    }

    try facet_sqlite.queueFacetDelta(db, 22, 1, "technology", 2, 1);
    try std.testing.expectEqual(@as(u64, 1), try facet_sqlite.mergeDeltasSafe(db, 22, null));

    try db.exec("ROLLBACK TO SAVEPOINT outer_facet_tx");
    try db.exec("RELEASE SAVEPOINT outer_facet_tx");

    var restored = try facet_sqlite.loadPostingBitmap(db, 22, 1, "technology", 0);
    defer restored.deinit();
    const ids = try restored.toArray(std.testing.allocator);
    defer std.testing.allocator.free(ids);
    try std.testing.expectEqualSlices(u32, &.{1}, ids);
    try std.testing.expectEqual(@as(u64, 0), try facet_sqlite.countFacetDeltas(db, 22, null));
}

test "facet sync orchestration queues and merges assignments" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try facet_sqlite.setupFacetTable(
        db,
        23,
        "public",
        "documents",
        4,
        &.{
            .{ .facet_id = 1, .facet_name = "category" },
            .{ .facet_id = 2, .facet_name = "region" },
        },
    );

    try std.testing.expectEqual(@as(u64, 0), try facet_sqlite.countFacetDeltas(db, 23, null));
    try std.testing.expectEqual(@as(u64, 2), try facet_sqlite.syncFacetAssignments(
        db,
        23,
        5,
        &.{
            .{ .facet_id = 1, .facet_value = "graph" },
            .{ .facet_id = 2, .facet_value = "eu" },
        },
    ));
    try std.testing.expectEqual(@as(u64, 0), try facet_sqlite.countFacetDeltas(db, 23, null));

    var category = try facet_sqlite.loadPostingBitmap(db, 23, 1, "graph", 0);
    defer category.deinit();
    const category_ids = try category.toArray(std.testing.allocator);
    defer std.testing.allocator.free(category_ids);
    try std.testing.expectEqualSlices(u32, &.{5}, category_ids);

    var region = try facet_sqlite.loadPostingBitmap(db, 23, 2, "eu", 0);
    defer region.deinit();
    const region_ids = try region.toArray(std.testing.allocator);
    defer std.testing.allocator.free(region_ids);
    try std.testing.expectEqualSlices(u32, &.{5}, region_ids);
}
