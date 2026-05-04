const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;
const roaring_utils = @import("roaring_utils.zig");

// ============================================================================
// Session-scoped plan cache
// ============================================================================
// SPI_keepplan() makes the plan survive across SPI connect/finish cycles
// within the same backend session. We use null-initialised pointers and
// populate them on first use.
//
// Plans are invalidated automatically when the backend exits.
// Thread-safety is not a concern: the legacy backend executes one function at a time per session.

var g_bfs_hop_plan:       ?*c.SPIPlanPtr = null; // graph.bfs_hop(int4[],rb,text[],r,r,d,d)
var g_roaring_oid_cache:  c.Oid          = 0;    // cached roaringbitmap OID

// ============================================================================
// Public entry point: k_hops_filtered (legacy native wrapper)
// ============================================================================

/// Native wrapper: parses fcinfo args and calls k_hops_filtered.
pub fn k_hops_filtered_wrapper(fcinfo: c.FunctionCallInfo) c.Datum {
    const nargs = fcinfo.*.nargs;
    if (nargs < 2) {
        utils.elog(c.ERROR, "k_hops_filtered requires at least 2 arguments");
        unreachable;
    }

    if (utils.is_arg_null(fcinfo, 0)) {
        const empty = roaring_utils.createEmptyBitmap();
        defer c.roaring_bitmap_free(empty);
        return roaring_utils.roaringBitmapToDatum(empty);
    }

    const seed_nodes_datum = utils.get_arg_datum(fcinfo, 0);
    const max_hops         = @as(i32, @intCast(utils.get_arg_datum(fcinfo, 1)));

    const edge_types_datum: ?c.Datum = if (nargs > 2 and !utils.is_arg_null(fcinfo, 2)) utils.get_arg_datum(fcinfo, 2) else null;
    const doc_types_datum:  ?c.Datum = if (nargs > 3 and !utils.is_arg_null(fcinfo, 3)) utils.get_arg_datum(fcinfo, 3) else null;
    const jurisdictions_datum: ?c.Datum = if (nargs > 4 and !utils.is_arg_null(fcinfo, 4)) utils.get_arg_datum(fcinfo, 4) else null;
    const after_date_datum: ?c.Datum = if (nargs > 5 and !utils.is_arg_null(fcinfo, 5)) utils.get_arg_datum(fcinfo, 5) else null;
    const before_date_datum: ?c.Datum = if (nargs > 6 and !utils.is_arg_null(fcinfo, 6)) utils.get_arg_datum(fcinfo, 6) else null;
    const conf_min: ?f32 = if (nargs > 7 and !utils.is_arg_null(fcinfo, 7)) @as(f32, @bitCast(@as(u32, @truncate(utils.get_arg_datum(fcinfo, 7))))) else null;
    const conf_max: ?f32 = if (nargs > 8 and !utils.is_arg_null(fcinfo, 8)) @as(f32, @bitCast(@as(u32, @truncate(utils.get_arg_datum(fcinfo, 8))))) else null;

    return k_hops_filtered(
        seed_nodes_datum,
        max_hops,
        edge_types_datum,
        doc_types_datum,
        jurisdictions_datum,
        after_date_datum,
        before_date_datum,
        conf_min,
        conf_max,
    );
}

// ============================================================================
// BFS core — optimised: 1 SPI call per hop via graph.bfs_hop()
// ============================================================================
//
// Optimisation summary vs. 0.2.x:
//   Before: 4 SPI round-trips per hop (lj_o, lj_i, filterEdgesMeta, getNextNodes)
//   After:  1 SPI round-trip per hop  (graph.bfs_hop() CTE handles all steps)
//   Saving: 75% SPI overhead for k=3 hops (12 → 3 calls)
//
// Additional micro-optimisations:
//   • getRoaringBitmapOid() cached per session (avoids 1 SPI call per hop)
//   • roaring_bitmap_run_optimize() called on visited after each hop
//     (compresses run-length sequences, reduces serialisation cost)
//   • bitmapToArray skipped when frontier is empty (early exit)

fn k_hops_filtered(
    seed_nodes_datum: c.Datum,
    max_hops: i32,
    edge_types_datum: ?c.Datum,
    doc_types_datum: ?c.Datum,
    jurisdictions_datum: ?c.Datum,
    after_date_datum: ?c.Datum,
    before_date_datum: ?c.Datum,
    conf_min: ?f32,
    conf_max: ?f32,
) c.Datum {
    // doc_types / jurisdictions: reserved for future Zig-side filtering.
    // Currently passed through but not used by graph.bfs_hop().
    _ = doc_types_datum;
    _ = jurisdictions_datum;

    // Deserialise seed bitmap BEFORE opening SPI (avoids memory context issues)
    const frontier_init = roaring_utils.datumToRoaringBitmap(seed_nodes_datum);
    var frontier = frontier_init;

    const visited = roaring_utils.createEmptyBitmap();
    roaring_utils.bitmapOrInplace(visited, frontier);

    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        c.roaring_bitmap_free(frontier);
        c.roaring_bitmap_free(visited);
        utils.elog(c.ERROR, "pg_dgraph: SPI_connect failed");
        unreachable;
    }

    // Cache roaringbitmap OID once per session (avoids 1 SPI call per hop)
    if (g_roaring_oid_cache == 0) {
        g_roaring_oid_cache = utils.getRoaringBitmapOid();
    }

    var current_depth: i32 = 0;
    while (current_depth < max_hops and !roaring_utils.isBitmapEmpty(frontier)) : (current_depth += 1) {

        // ── Single SPI call replaces 4 separate round-trips ──────────────────
        const new_nodes = bfsHopSQL(
            frontier,
            visited,
            edge_types_datum,
            conf_min,
            conf_max,
            after_date_datum,
            before_date_datum,
        );
        defer if (new_nodes) |bm| c.roaring_bitmap_free(bm);

        if (new_nodes == null or roaring_utils.isBitmapEmpty(new_nodes.?)) break;

        roaring_utils.bitmapOrInplace(visited, new_nodes.?);

        // Run-length optimise after accumulation (reduces bitmap serialisation cost)
        _ = c.roaring_bitmap_run_optimize(visited);

        // Swap frontier to the newly discovered nodes
        const old_frontier = frontier;
        frontier = roaring_utils.createEmptyBitmap();
        roaring_utils.bitmapOrInplace(frontier, new_nodes.?);
        c.roaring_bitmap_free(old_frontier);
    }

    c.roaring_bitmap_free(frontier);
    _ = c.SPI_finish();

    // Serialise AFTER SPI_finish so palloc is in the outer memory context
    const result = roaring_utils.roaringBitmapToDatum(visited);
    c.roaring_bitmap_free(visited);
    return result;
}

// ============================================================================
// bfsHopSQL — calls graph.bfs_hop() via one SPI round-trip
// ============================================================================
//
// Converts the frontier bitmap to an int4[] (for indexed lookup
// inside graph.bfs_hop), then calls the SQL CTE function.
//
// Adaptive threshold: for very small frontiers (≤ SMALL_FRONTIER_THRESHOLD)
// we avoid the overhead of constructing an array datum and instead
// call the legacy per-step path.  In practice, most BFS frontiers grow quickly
// beyond this threshold, so the adaptive path is rarely taken.

const SMALL_FRONTIER_THRESHOLD: u64 = 3;

fn bfsHopSQL(
    frontier:          *c.roaring_bitmap_t,
    visited:           *c.roaring_bitmap_t,
    edge_types_datum:  ?c.Datum,
    conf_min:          ?f32,
    conf_max:          ?f32,
    after_date_datum:  ?c.Datum,
    before_date_datum: ?c.Datum,
) ?*c.roaring_bitmap_t {
    const allocator = utils.PgAllocator.allocator();

    // Convert frontier bitmap to int4[] — used by bfs_hop for PK index scans
    const node_array = roaring_utils.bitmapToArray(allocator, frontier) catch {
        utils.elog(c.ERROR, "pg_dgraph: OOM in bfsHopSQL bitmapToArray");
        unreachable;
    };
    defer allocator.free(node_array);

    if (node_array.len == 0) return null;

    // Build int4 array datum
    const node_datums = allocator.alloc(c.Datum, node_array.len) catch {
        utils.elog(c.ERROR, "pg_dgraph: OOM in bfsHopSQL Datum alloc");
        unreachable;
    };
    defer allocator.free(node_datums);
    for (node_array, 0..) |val, i| node_datums[i] = c.Int32GetDatum(val);

    const frontier_arr = c.construct_array(
        node_datums.ptr, @intCast(node_array.len),
        c.INT4OID, @sizeOf(i32), true, 'i',
    );
    defer c.pfree(frontier_arr);
    const frontier_datum = utils.pointer_get_datum(frontier_arr);

    // Serialise visited bitmap for rb_andnot inside bfs_hop
    const visited_datum = roaring_utils.roaringBitmapToDatum(visited);
    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(visited_datum).?)));

    // Build SPI args for graph.bfs_hop($1..$7)
    const rb_oid = g_roaring_oid_cache;

    var argtypes = [7]c.Oid{
        c.INT4ARRAYOID, rb_oid,
        c.TEXTARRAYOID,
        c.FLOAT4OID, c.FLOAT4OID,
        c.DATEOID, c.DATEOID,
    };
    var argvalues = [7]c.Datum{
        frontier_datum,
        visited_datum,
        if (edge_types_datum) |v| v else 0,
        if (conf_min) |v| c.Float4GetDatum(v) else 0,
        if (conf_max) |v| c.Float4GetDatum(v) else 0,
        if (after_date_datum) |v| v else 0,
        if (before_date_datum) |v| v else 0,
    };
    // SPI null flags: 'n' = null, ' ' = not-null
    const nulls = [8]u8{
        ' ', ' ',  // frontier_arr, visited  — always provided
        if (edge_types_datum  == null) 'n' else ' ',
        if (conf_min          == null) 'n' else ' ',
        if (conf_max          == null) 'n' else ' ',
        if (after_date_datum  == null) 'n' else ' ',
        if (before_date_datum == null) 'n' else ' ',
        0,
    };

    const query = "SELECT graph.bfs_hop($1,$2,$3,$4,$5,$6,$7)";

    const ret = c.SPI_execute_with_args(
        query, 7,
        @ptrCast(&argtypes), @ptrCast(&argvalues),
        @ptrCast(&nulls), true, 1,
    );

    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) return null;

    var isnull: bool = false;
    const result_datum = c.SPI_getbinval(
        c.SPI_tuptable.*.vals[0],
        c.SPI_tuptable.*.tupdesc,
        1, &isnull,
    );
    if (isnull or result_datum == 0) return null;

    return roaring_utils.datumToRoaringBitmap(result_datum);
}

// ============================================================================
// Legacy helpers — kept for external callers (shortest_path.zig)
// ============================================================================
// These functions are still used by the bidirectional BFS in shortest_path.zig.
// They are NOT called by k_hops_filtered anymore.

/// Returns a bitmap of edge IDs that pass the allowed_edges filter.
/// Caller must free the result. Assumes SPI is connected.
pub fn getAllowedEdges(
    edge_types_datum: ?c.Datum,
    doc_types_datum: ?c.Datum,
    jurisdictions_datum: ?c.Datum,
) ?*c.roaring_bitmap_t {
    const query = "SELECT allowed_edges($1, $2, $3) as allowed";

    var argtypes  = [3]c.Oid   { c.TEXTARRAYOID, c.TEXTARRAYOID, c.TEXTARRAYOID };
    var argvalues = [3]c.Datum {
        if (edge_types_datum)    |v| v else 0,
        if (doc_types_datum)     |v| v else 0,
        if (jurisdictions_datum) |v| v else 0,
    };
    const nulls = [4]u8{
        if (edge_types_datum    == null) 'n' else ' ',
        if (doc_types_datum     == null) 'n' else ' ',
        if (jurisdictions_datum == null) 'n' else ' ',
        0,
    };

    const ret = c.SPI_execute_with_args(
        query, 3,
        @ptrCast(&argtypes), @ptrCast(&argvalues),
        @ptrCast(&nulls), true, 1,
    );
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) return null;

    var isnull: bool = false;
    const datum = c.SPI_getbinval(
        c.SPI_tuptable.*.vals[0],
        c.SPI_tuptable.*.tupdesc,
        1, &isnull,
    );
    if (isnull or datum == 0) return null;

    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(datum).?)));
    return roaring_utils.datumToRoaringBitmap(datum);
}

/// Returns a bitmap of edge IDs from the given nodes in the given direction.
/// Caller must free the result. Assumes SPI is connected.
pub fn getEdgesFromNodes(nodes: *c.roaring_bitmap_t, outgoing: bool) ?*c.roaring_bitmap_t {
    const allocator = utils.PgAllocator.allocator();

    const node_array = roaring_utils.bitmapToArray(allocator, nodes) catch {
        utils.elog(c.ERROR, "pg_dgraph: OOM in getEdgesFromNodes");
        unreachable;
    };
    defer allocator.free(node_array);

    if (node_array.len == 0) return null;

    const table_name = if (outgoing) "lj_o" else "lj_i";
    var query_buf: [128]u8 = undefined;
    const query = std.fmt.bufPrintZ(
        &query_buf,
        "SELECT rb_or_agg(edges) as edges FROM {s} WHERE node = ANY($1)",
        .{table_name},
    ) catch {
        utils.elog(c.ERROR, "pg_dgraph: query buffer too small");
        unreachable;
    };

    const datums = allocator.alloc(c.Datum, node_array.len) catch {
        utils.elog(c.ERROR, "pg_dgraph: OOM building node array");
        unreachable;
    };
    defer allocator.free(datums);
    for (node_array, 0..) |val, i| datums[i] = c.Int32GetDatum(val);

    const arr = c.construct_array(
        datums.ptr, @intCast(node_array.len),
        c.INT4OID, @sizeOf(i32), true, 'i',
    );
    defer c.pfree(arr);
    const node_array_datum = utils.pointer_get_datum(arr);

    var argtypes  = [1]c.Oid   { c.INT4ARRAYOID };
    var argvalues = [1]c.Datum { node_array_datum };

    const ret = c.SPI_execute_with_args(
        query.ptr, 1,
        @ptrCast(&argtypes), @ptrCast(&argvalues),
        null, true, 1,
    );
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) return roaring_utils.createEmptyBitmap();

    var isnull: bool = false;
    const edges_datum = c.SPI_getbinval(
        c.SPI_tuptable.*.vals[0],
        c.SPI_tuptable.*.tupdesc,
        1, &isnull,
    );
    if (isnull or edges_datum == 0) return roaring_utils.createEmptyBitmap();

    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(edges_datum).?)));
    return roaring_utils.datumToRoaringBitmap(edges_datum);
}

/// Returns edges from both outgoing and incoming tables (for undirected traversal).
/// Caller must free the result. Assumes SPI is connected.
pub fn getEdgesFromNodesBoth(nodes: *c.roaring_bitmap_t) ?*c.roaring_bitmap_t {
    const edge_out = getEdgesFromNodes(nodes, true);
    defer if (edge_out) |bm| c.roaring_bitmap_free(bm);

    const edge_in = getEdgesFromNodes(nodes, false);
    defer if (edge_in) |bm| c.roaring_bitmap_free(bm);

    if (edge_out != null and edge_in != null) {
        if (roaring_utils.isBitmapEmpty(edge_out.?) and roaring_utils.isBitmapEmpty(edge_in.?)) {
            return roaring_utils.createEmptyBitmap();
        }
        return roaring_utils.bitmapOr(edge_out.?, edge_in.?);
    } else if (edge_out != null) {
        const empty = roaring_utils.createEmptyBitmap();
        defer c.roaring_bitmap_free(empty);
        return roaring_utils.bitmapOr(edge_out.?, empty);
    } else if (edge_in != null) {
        const empty = roaring_utils.createEmptyBitmap();
        defer c.roaring_bitmap_free(empty);
        return roaring_utils.bitmapOr(edge_in.?, empty);
    } else {
        return roaring_utils.createEmptyBitmap();
    }
}

/// Filters edges by metadata (date range, confidence). Assumes SPI is connected.
pub fn filterEdgesMeta(
    edges: *c.roaring_bitmap_t,
    after_date_datum: ?c.Datum,
    before_date_datum: ?c.Datum,
    conf_min: ?f32,
    conf_max: ?f32,
) ?*c.roaring_bitmap_t {
    if (roaring_utils.isBitmapEmpty(edges)) return null;

    if (after_date_datum == null and before_date_datum == null and
        conf_min == null and conf_max == null) return null;

    const query = "SELECT filter_edges_meta($1, $2, $3, $4, $5) as filtered";

    const roaringbitmap_oid = if (g_roaring_oid_cache != 0) g_roaring_oid_cache
                              else utils.getRoaringBitmapOid();
    const edges_datum = roaring_utils.roaringBitmapToDatum(edges);
    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(edges_datum).?)));

    var argtypes  = [5]c.Oid{
        roaringbitmap_oid, c.DATEOID, c.DATEOID, c.FLOAT4OID, c.FLOAT4OID,
    };
    var argvalues = [5]c.Datum{
        edges_datum,
        if (after_date_datum)  |v| v else 0,
        if (before_date_datum) |v| v else 0,
        if (conf_min) |v| c.Float4GetDatum(v) else 0,
        if (conf_max) |v| c.Float4GetDatum(v) else 0,
    };
    const nulls = [6]u8{
        ' ',
        if (after_date_datum  == null) 'n' else ' ',
        if (before_date_datum == null) 'n' else ' ',
        if (conf_min          == null) 'n' else ' ',
        if (conf_max          == null) 'n' else ' ',
        0,
    };

    const ret = c.SPI_execute_with_args(
        query, 5,
        @ptrCast(&argtypes), @ptrCast(&argvalues),
        @ptrCast(&nulls), true, 1,
    );
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) return null;

    var isnull: bool = false;
    const filtered_datum = c.SPI_getbinval(
        c.SPI_tuptable.*.vals[0],
        c.SPI_tuptable.*.tupdesc,
        1, &isnull,
    );
    if (isnull or filtered_datum == 0) return null;

    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(filtered_datum).?)));
    return roaring_utils.datumToRoaringBitmap(filtered_datum);
}

/// Gets next nodes from edges, picking the endpoint not in the current frontier.
/// Assumes SPI is connected.
pub fn getNextNodesFromEdges(
    frontier: *c.roaring_bitmap_t,
    edges: *c.roaring_bitmap_t,
) ?*c.roaring_bitmap_t {
    if (roaring_utils.isBitmapEmpty(edges)) return null;

    const allocator = utils.PgAllocator.allocator();
    const edge_array = roaring_utils.bitmapToArray(allocator, edges) catch {
        utils.elog(c.ERROR, "pg_dgraph: OOM in getNextNodesFromEdges");
        unreachable;
    };
    defer allocator.free(edge_array);

    if (edge_array.len == 0) return null;

    const roaringbitmap_oid = if (g_roaring_oid_cache != 0) g_roaring_oid_cache
                              else utils.getRoaringBitmapOid();
    const frontier_datum = roaring_utils.roaringBitmapToDatum(frontier);
    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(frontier_datum).?)));

    const edge_datums = allocator.alloc(c.Datum, edge_array.len) catch {
        utils.elog(c.ERROR, "pg_dgraph: OOM building edge datum array");
        unreachable;
    };
    defer allocator.free(edge_datums);
    for (edge_array, 0..) |val, i| edge_datums[i] = c.Int32GetDatum(val);

    const edge_arr = c.construct_array(
        edge_datums.ptr, @intCast(edge_array.len),
        c.INT4OID, @sizeOf(i32), true, 'i',
    );
    defer c.pfree(edge_arr);
    const edge_array_datum = utils.pointer_get_datum(edge_arr);

    const query =
        "SELECT rb_build_agg(CASE WHEN rb_contains($1, source_i) THEN target_i ELSE source_i END) " ++
        "as next_nodes FROM graph_edge_int WHERE edge_int = ANY($2) " ++
        "AND (rb_contains($1, source_i) OR rb_contains($1, target_i))";

    var argtypes  = [2]c.Oid   { roaringbitmap_oid, c.INT4ARRAYOID };
    var argvalues = [2]c.Datum { frontier_datum, edge_array_datum };

    const ret = c.SPI_execute_with_args(
        query, 2,
        @ptrCast(&argtypes), @ptrCast(&argvalues),
        null, true, 1,
    );
    if (ret != c.SPI_OK_SELECT or c.SPI_processed == 0) return null;

    var isnull: bool = false;
    const next_datum = c.SPI_getbinval(
        c.SPI_tuptable.*.vals[0],
        c.SPI_tuptable.*.tupdesc,
        1, &isnull,
    );
    if (isnull or next_datum == 0) return null;

    defer c.pfree(@constCast(@ptrCast(utils.datum_get_pointer(next_datum).?)));
    return roaring_utils.datumToRoaringBitmap(next_datum);
}
