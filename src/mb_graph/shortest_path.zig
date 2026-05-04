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

    const src = @as(i32, @intCast(utils.get_arg_datum(fcinfo, 0)));
    const dest = @as(i32, @intCast(utils.get_arg_datum(fcinfo, 1)));

    const edge_types_datum: ?c.Datum = if (nargs > 2 and !utils.is_arg_null(fcinfo, 2)) utils.get_arg_datum(fcinfo, 2) else null;
    const doc_types_datum: ?c.Datum = if (nargs > 3 and !utils.is_arg_null(fcinfo, 3)) utils.get_arg_datum(fcinfo, 3) else null;
    const jurisdictions_datum: ?c.Datum = if (nargs > 4 and !utils.is_arg_null(fcinfo, 4)) utils.get_arg_datum(fcinfo, 4) else null;
    const after_date_datum: ?c.Datum = if (nargs > 5 and !utils.is_arg_null(fcinfo, 5)) utils.get_arg_datum(fcinfo, 5) else null;
    const before_date_datum: ?c.Datum = if (nargs > 6 and !utils.is_arg_null(fcinfo, 6)) utils.get_arg_datum(fcinfo, 6) else null;
    const conf_min: ?f32 = if (nargs > 7 and !utils.is_arg_null(fcinfo, 7)) @as(f32, @bitCast(@as(u32, @truncate(utils.get_arg_datum(fcinfo, 7))))) else null;
    const conf_max: ?f32 = if (nargs > 8 and !utils.is_arg_null(fcinfo, 8)) @as(f32, @bitCast(@as(u32, @truncate(utils.get_arg_datum(fcinfo, 8))))) else null;
    const max_depth: i32 = if (nargs > 9 and !utils.is_arg_null(fcinfo, 9)) @as(i32, @intCast(utils.get_arg_datum(fcinfo, 9))) else 20;

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

    var o_depth: i32 = 0;
    var o_new = roaring_utils.createBitmapFromArray(&.{src});
    var o_tc = roaring_utils.createBitmapFromArray(&.{src});

    var i_depth: i32 = 0;
    var i_new = roaring_utils.createBitmapFromArray(&.{dest});
    var i_tc = roaring_utils.createBitmapFromArray(&.{dest});

    const allowed = graph_traversal.getAllowedEdges(edge_types_datum, doc_types_datum, jurisdictions_datum);

    const result: ?i32 = blk: {
        while (true) {
            const overlap = roaring_utils.bitmapAnd(i_new, o_new);
            defer c.roaring_bitmap_free(overlap);

            if (!roaring_utils.isBitmapEmpty(overlap)) {
                break :blk i_depth + o_depth;
            }

            if (o_depth + i_depth >= max_depth) {
                break :blk null;
            }

            const i_card = roaring_utils.bitmapCardinality(i_new);
            const o_card = roaring_utils.bitmapCardinality(o_new);

            if (i_card < o_card) {
                // Expand inward frontier
                const e_front = graph_traversal.getEdgesFromNodesBoth(i_new);
                if (e_front == null or roaring_utils.isBitmapEmpty(e_front.?)) {
                    if (e_front) |bm| c.roaring_bitmap_free(bm);
                    break :blk null;
                }

                const e_filt = if (allowed) |a| roaring_utils.bitmapAnd(e_front.?, a) else e_front.?;
                if (allowed != null) c.roaring_bitmap_free(e_front.?);
                defer if (allowed != null) c.roaring_bitmap_free(e_filt);

                const e_filt_meta = graph_traversal.filterEdgesMeta(e_filt, after_date_datum, before_date_datum, conf_min, conf_max);
                defer if (e_filt_meta) |bm| c.roaring_bitmap_free(bm);

                if (e_filt_meta == null or roaring_utils.isBitmapEmpty(e_filt_meta.?)) break :blk null;

                const n_next = graph_traversal.getNextNodesFromEdges(i_new, e_filt_meta.?);
                defer if (n_next) |bm| c.roaring_bitmap_free(bm);

                if (n_next == null or roaring_utils.isBitmapEmpty(n_next.?)) break :blk null;

                const i_new_filtered = roaring_utils.bitmapDifference(n_next.?, i_tc);
                defer c.roaring_bitmap_free(i_new_filtered);

                if (roaring_utils.isBitmapEmpty(i_new_filtered)) break :blk null;

                const old_i_new = i_new;
                i_new = roaring_utils.createEmptyBitmap();
                roaring_utils.bitmapOrInplace(i_new, i_new_filtered);
                c.roaring_bitmap_free(old_i_new);

                const temp_i_tc = roaring_utils.bitmapOr(i_tc, i_new);
                c.roaring_bitmap_free(i_tc);
                i_tc = temp_i_tc;
                i_depth += 1;
            } else {
                // Expand outward frontier
                const e_front = graph_traversal.getEdgesFromNodesBoth(o_new);
                if (e_front == null or roaring_utils.isBitmapEmpty(e_front.?)) {
                    if (e_front) |bm| c.roaring_bitmap_free(bm);
                    break :blk null;
                }

                const e_filt = if (allowed) |a| roaring_utils.bitmapAnd(e_front.?, a) else e_front.?;
                if (allowed != null) c.roaring_bitmap_free(e_front.?);
                defer if (allowed != null) c.roaring_bitmap_free(e_filt);

                const e_filt_meta = graph_traversal.filterEdgesMeta(e_filt, after_date_datum, before_date_datum, conf_min, conf_max);
                defer if (e_filt_meta) |bm| c.roaring_bitmap_free(bm);

                if (e_filt_meta == null or roaring_utils.isBitmapEmpty(e_filt_meta.?)) break :blk null;

                const n_next = graph_traversal.getNextNodesFromEdges(o_new, e_filt_meta.?);
                defer if (n_next) |bm| c.roaring_bitmap_free(bm);

                if (n_next == null or roaring_utils.isBitmapEmpty(n_next.?)) break :blk null;

                const o_new_filtered = roaring_utils.bitmapDifference(n_next.?, o_tc);
                defer c.roaring_bitmap_free(o_new_filtered);

                if (roaring_utils.isBitmapEmpty(o_new_filtered)) break :blk null;

                const old_o_new = o_new;
                o_new = roaring_utils.createEmptyBitmap();
                roaring_utils.bitmapOrInplace(o_new, o_new_filtered);
                c.roaring_bitmap_free(old_o_new);

                const temp_o_tc = roaring_utils.bitmapOr(o_tc, o_new);
                c.roaring_bitmap_free(o_tc);
                o_tc = temp_o_tc;
                o_depth += 1;
            }
        }
    };

    if (allowed) |bm| c.roaring_bitmap_free(bm);
    c.roaring_bitmap_free(o_new);
    c.roaring_bitmap_free(o_tc);
    c.roaring_bitmap_free(i_new);
    c.roaring_bitmap_free(i_tc);
    _ = c.SPI_finish();

    return result;
}
