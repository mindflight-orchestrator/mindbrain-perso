const utils = @import("utils.zig");
const c = utils.c;
const roaring_utils = @import("roaring_utils.zig");
const graph_traversal = @import("graph_traversal.zig");

/// Native wrapper: parses fcinfo args and calls shortest_path_filtered.
pub fn shortest_path_filtered_wrapper(fcinfo: c.FunctionCallInfo) c.Datum {
    const nargs = fcinfo.*.nargs;
    if (nargs < 2) {
        utils.elog(c.ERROR, "shortest_path_filtered requires at least 2 arguments");
        unreachable;
    }

    const src = utils.datum_get_int32(utils.get_arg_datum(fcinfo, 0));
    const dest = utils.datum_get_int32(utils.get_arg_datum(fcinfo, 1));
    if (src < 0 or dest < 0) {
        utils.elog(c.ERROR, "shortest_path_filtered: node ids must be non-negative");
        unreachable;
    }

    const edge_types_datum: ?c.Datum = if (nargs > 2 and !utils.is_arg_null(fcinfo, 2)) utils.get_arg_datum(fcinfo, 2) else null;
    const doc_types_datum: ?c.Datum = if (nargs > 3 and !utils.is_arg_null(fcinfo, 3)) utils.get_arg_datum(fcinfo, 3) else null;
    const jurisdictions_datum: ?c.Datum = if (nargs > 4 and !utils.is_arg_null(fcinfo, 4)) utils.get_arg_datum(fcinfo, 4) else null;
    const after_date_datum: ?c.Datum = if (nargs > 5 and !utils.is_arg_null(fcinfo, 5)) utils.get_arg_datum(fcinfo, 5) else null;
    const before_date_datum: ?c.Datum = if (nargs > 6 and !utils.is_arg_null(fcinfo, 6)) utils.get_arg_datum(fcinfo, 6) else null;
    const conf_min: ?f32 = if (nargs > 7 and !utils.is_arg_null(fcinfo, 7)) @as(f32, @bitCast(@as(u32, @truncate(utils.get_arg_datum(fcinfo, 7))))) else null;
    const conf_max: ?f32 = if (nargs > 8 and !utils.is_arg_null(fcinfo, 8)) @as(f32, @bitCast(@as(u32, @truncate(utils.get_arg_datum(fcinfo, 8))))) else null;
    const max_depth: i32 = if (nargs > 9 and !utils.is_arg_null(fcinfo, 9)) utils.datum_get_int32(utils.get_arg_datum(fcinfo, 9)) else 20;
    if (max_depth < 0) {
        utils.elog(c.ERROR, "shortest_path_filtered: max_depth must be non-negative");
        unreachable;
    }

    const result = shortest_path_filtered(
        src, dest,
        edge_types_datum, doc_types_datum, jurisdictions_datum,
        after_date_datum, before_date_datum,
        conf_min, conf_max,
        max_depth,
    );

    if (result) |r| {
        return c.Int32GetDatum(r);
    } else {
        utils.set_return_null(fcinfo);
        return 0;
    }
}

const max_layer_slots: usize = 64;

const SearchSide = struct {
    depth: i32 = 0,
    frontier: *c.roaring_bitmap_t,
    closure: *c.roaring_bitmap_t,
    // One bitmap per BFS layer so a meet with any layer of the opposite
    // side yields the exact hop count; intersecting only the two current
    // frontiers missed (or overestimated) meets with older layers when one
    // side advanced several levels in a row.
    layers: [max_layer_slots]?*c.roaring_bitmap_t = @splat(null),

    fn deinit(self: *SearchSide) void {
        c.roaring_bitmap_free(self.frontier);
        c.roaring_bitmap_free(self.closure);
        for (self.layers) |maybe_layer| {
            if (maybe_layer) |layer| c.roaring_bitmap_free(layer);
        }
    }

    /// Smallest opposite-layer depth intersecting `frontier`, if any.
    fn meetDepth(self: *const SearchSide, frontier: *c.roaring_bitmap_t) ?i32 {
        for (self.layers, 0..) |maybe_layer, layer_depth| {
            const layer = maybe_layer orelse break;
            const overlap = roaring_utils.bitmapAnd(frontier, layer);
            defer c.roaring_bitmap_free(overlap);
            if (!roaring_utils.isBitmapEmpty(overlap)) return @intCast(layer_depth);
        }
        return null;
    }
};

fn initSide(start: i32) SearchSide {
    var side = SearchSide{
        .frontier = roaring_utils.createBitmapFromArray(&.{start}),
        .closure = roaring_utils.createBitmapFromArray(&.{start}),
    };
    side.layers[0] = roaring_utils.createBitmapFromArray(&.{start});
    return side;
}

/// Expands `side` one BFS level. Returns false when the search is exhausted.
fn expandSide(
    side: *SearchSide,
    allowed: ?*c.roaring_bitmap_t,
    has_meta_filters: bool,
    after_date_datum: ?c.Datum,
    before_date_datum: ?c.Datum,
    conf_min: ?f32,
    conf_max: ?f32,
) bool {
    const e_front = graph_traversal.getEdgesFromNodesBoth(side.frontier);
    if (e_front == null or roaring_utils.isBitmapEmpty(e_front.?)) {
        if (e_front) |bm| c.roaring_bitmap_free(bm);
        return false;
    }
    // e_front is always freed here; when allowed == null, e_filt merely
    // aliases it (the alias-gated free leaked one bitmap per BFS level for
    // the lifetime of the backend on unfiltered calls).
    defer c.roaring_bitmap_free(e_front.?);

    const e_filt = if (allowed) |a| roaring_utils.bitmapAnd(e_front.?, a) else e_front.?;
    defer if (allowed != null) c.roaring_bitmap_free(e_filt);

    // filterEdgesMeta returns null both for "no result" and "no filters";
    // only consult it when filters were actually supplied, otherwise every
    // unfiltered call was treated as "no path".
    var meta_owned: ?*c.roaring_bitmap_t = null;
    defer if (meta_owned) |bm| c.roaring_bitmap_free(bm);
    const edges = if (has_meta_filters) blk: {
        meta_owned = graph_traversal.filterEdgesMeta(e_filt, after_date_datum, before_date_datum, conf_min, conf_max);
        if (meta_owned == null or roaring_utils.isBitmapEmpty(meta_owned.?)) return false;
        break :blk meta_owned.?;
    } else e_filt;

    const n_next = graph_traversal.getNextNodesFromEdges(side.frontier, edges);
    defer if (n_next) |bm| c.roaring_bitmap_free(bm);
    if (n_next == null or roaring_utils.isBitmapEmpty(n_next.?)) return false;

    const fresh = roaring_utils.bitmapDifference(n_next.?, side.closure);
    if (roaring_utils.isBitmapEmpty(fresh)) {
        c.roaring_bitmap_free(fresh);
        return false;
    }

    c.roaring_bitmap_free(side.frontier);
    side.frontier = fresh;

    const merged = roaring_utils.bitmapOr(side.closure, side.frontier);
    c.roaring_bitmap_free(side.closure);
    side.closure = merged;

    side.depth += 1;
    const slot: usize = @intCast(side.depth);
    if (slot < max_layer_slots) {
        side.layers[slot] = c.roaring_bitmap_copy(side.frontier);
    }
    return true;
}

/// Bidirectional BFS shortest path. Returns hop count or null if no path found.
fn shortest_path_filtered(
    src: i32,
    dest: i32,
    edge_types_datum: ?c.Datum,
    doc_types_datum: ?c.Datum,
    jurisdictions_datum: ?c.Datum,
    after_date_datum: ?c.Datum,
    before_date_datum: ?c.Datum,
    conf_min: ?f32,
    conf_max: ?f32,
    max_depth: i32,
) ?i32 {
    if (src == dest) return 0;

    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "pg_dgraph: SPI_connect failed");
        unreachable;
    }

    const depth_cap: i32 = @min(max_depth, @as(i32, max_layer_slots - 1));

    var outward = initSide(src);
    var inward = initSide(dest);
    const has_meta_filters = after_date_datum != null or before_date_datum != null or
        conf_min != null or conf_max != null;

    const allowed = graph_traversal.getAllowedEdges(edge_types_datum, doc_types_datum, jurisdictions_datum);

    const result: ?i32 = blk: {
        while (true) {
            if (outward.depth + inward.depth >= depth_cap) break :blk null;

            const expand_inward = roaring_utils.bitmapCardinality(inward.frontier) <
                roaring_utils.bitmapCardinality(outward.frontier);
            const side = if (expand_inward) &inward else &outward;
            const opposite = if (expand_inward) &outward else &inward;

            if (!expandSide(side, allowed, has_meta_filters, after_date_datum, before_date_datum, conf_min, conf_max)) {
                break :blk null;
            }
            if (opposite.meetDepth(side.frontier)) |met_depth| {
                break :blk side.depth + met_depth;
            }
        }
    };

    if (allowed) |bm| c.roaring_bitmap_free(bm);
    outward.deinit();
    inward.deinit();
    _ = c.SPI_finish();

    return result;
}
