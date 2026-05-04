const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;
const PgAllocator = utils.PgAllocator;

const FacetCount = struct {
    facet_name: []const u8,
    facet_value: []const u8,
    cardinality: u64,
    facet_id: i32,
};

pub fn get_facet_counts_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    const allocator = PgAllocator.allocator();

    // Arguments:
    // 0: table_id (oid)
    // 1: filter_bitmap (roaringbitmap) (nullable)
    // 2: facets (text[]) (nullable)
    // 3: top_n (int) (default 5)

    const table_id_datum = utils.get_arg_datum(fcinfo, 0);
    const table_id: c.Oid = @intCast(table_id_datum);

    var filter_bitmap: ?*c.roaring_bitmap_t = null;
    if (!utils.is_arg_null(fcinfo, 1)) {
        const datum = utils.get_arg_datum(fcinfo, 1);
        const ptr = utils.detoast_datum(datum);
        const len = utils.varsize(ptr) - utils.varhdrsz();
        const data = utils.vardata(ptr);
        filter_bitmap = c.roaring_bitmap_portable_deserialize_safe(data, @intCast(len));
    }
    defer if (filter_bitmap) |bm| c.roaring_bitmap_free(bm);

    var target_facets = std.ArrayList([]const u8).empty;
    defer target_facets.deinit(allocator);

    if (!utils.is_arg_null(fcinfo, 2)) {
        const datum = utils.get_arg_datum(fcinfo, 2);
        const array = @as(*c.ArrayType, @alignCast(@ptrCast(utils.detoast_datum(datum))));
        const elemtype = c.ARR_ELEMTYPE(array);
        var elmlen: i16 = undefined;
        var elmbyval: bool = undefined;
        var elmalign: u8 = undefined;
        c.get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);

        var elems_datum_ptr: [*c]c.Datum = undefined;
        var elems_null_ptr: [*c]bool = undefined;
        var nelems: c_int = undefined;

        c.deconstruct_array(array, elemtype, elmlen, elmbyval, @intCast(elmalign), &elems_datum_ptr, &elems_null_ptr, &nelems);

        const elems_datum: [*]c.Datum = elems_datum_ptr;
        const elems_null: [*]bool = elems_null_ptr;

        var i: usize = 0;
        while (i < nelems) : (i += 1) {
            if (!elems_null[i]) {
                const s = c.TextDatumGetCString(elems_datum[i]);
                const len = std.mem.len(s);
                const s_copy = allocator.alloc(u8, len) catch unreachable;
                @memcpy(s_copy, s[0..len]);
                target_facets.append(allocator, s_copy) catch unreachable;
            }
        }
    }

    const limit: i32 = if (!utils.is_arg_null(fcinfo, 3)) @intCast(c.DatumGetInt32(utils.get_arg_datum(fcinfo, 3))) else 5;

    // ReturnSetInfo setup
    const rsi_ptr = @as(?*c.ReturnSetInfo, @ptrCast(@alignCast(fcinfo.*.resultinfo)));
    if (rsi_ptr == null or !utils.isA(@ptrCast(rsi_ptr), utils.tReturnSetInfo())) {
        utils.elog(c.ERROR, "set-valued function called in context that cannot accept a set");
    }
    const rsi = rsi_ptr.?;

    rsi.returnMode = c.SFRM_Materialize;

    var tupdesc: c.TupleDesc = undefined;
    if (c.get_call_result_type(fcinfo, null, &tupdesc) != c.TYPEFUNC_COMPOSITE) {
        utils.elog(c.ERROR, "return type must be a row type");
    }

    const oldcontext = c.MemoryContextSwitchTo(rsi.econtext.?.*.ecxt_per_query_memory);
    rsi.setResult = c.tuplestore_begin_heap(true, false, utils.workMem());
    rsi.setDesc = c.CreateTupleDescCopy(tupdesc);
    _ = c.MemoryContextSwitchTo(oldcontext);

    if (c.SPI_connect() != c.SPI_OK_CONNECT) {
        utils.elog(c.ERROR, "SPI_connect failed");
    }
    defer _ = c.SPI_finish();

    // Get table info, including chunk_bits so we can reconstruct full document ids.
    const table_info_query = std.fmt.allocPrintSentinel(allocator, "SELECT schemaname, facets_table, chunk_bits FROM facets.faceted_table WHERE table_id = {d}", .{table_id}, 0) catch unreachable;
    if (c.SPI_execute(table_info_query.ptr, true, 1) != c.SPI_OK_SELECT or c.SPI_processed != 1) {
        utils.elog(c.ERROR, "Table not found in facets.faceted_table");
    }

    const schema_name = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1);
    const facets_table = c.SPI_getvalue(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 2);
    var isnull_chunk_bits: bool = false;
    const chunk_bits_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 3, &isnull_chunk_bits);
    const chunk_bits: u5 = if (isnull_chunk_bits) 20 else @intCast(c.DatumGetInt32(chunk_bits_datum));

    // Get all facet definitions if not specified
    const FacetDef = struct { id: i32, name: []const u8 };
    var facets_to_process = std.ArrayList(FacetDef).empty;
    defer facets_to_process.deinit(allocator);

    if (target_facets.items.len > 0) {
        for (target_facets.items) |fname| {
            const f_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id FROM facets.facet_definition WHERE table_id = {d} AND facet_name = $1", .{table_id}, 0) catch unreachable;
            var f_argtypes = [_]c.Oid{c.TEXTOID};
            var f_values = [_]c.Datum{c.PointerGetDatum(c.cstring_to_text_with_len(fname.ptr, @intCast(fname.len)))};
            var f_nulls = [_]u8{' '};

            if (c.SPI_execute_with_args(f_query.ptr, 1, &f_argtypes, &f_values, &f_nulls, true, 1) == c.SPI_OK_SELECT and c.SPI_processed > 0) {
                var isnull_fid: bool = false;
                const fid_datum = c.SPI_getbinval(c.SPI_tuptable.*.vals[0], c.SPI_tuptable.*.tupdesc, 1, &isnull_fid);
                facets_to_process.append(allocator, .{ .id = c.DatumGetInt32(fid_datum), .name = fname }) catch unreachable;
            }
        }
    } else {
        const all_query = std.fmt.allocPrintSentinel(allocator, "SELECT facet_id, facet_name FROM facets.facet_definition WHERE table_id = {d}", .{table_id}, 0) catch unreachable;
        if (c.SPI_execute(all_query.ptr, true, 0) == c.SPI_OK_SELECT) {
            const proc = c.SPI_processed;
            var k: u64 = 0;
            while (k < proc) : (k += 1) {
                const tuple = c.SPI_tuptable.*.vals[k];
                const desc = c.SPI_tuptable.*.tupdesc;
                var isnull_fid: bool = false;
                var isnull_fname: bool = false;
                const fid_datum = c.SPI_getbinval(tuple, desc, 1, &isnull_fid);
                const fname_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_fname);
                const fname = c.TextDatumGetCString(fname_datum);

                const flen = std.mem.len(fname);
                const fcopy = allocator.alloc(u8, flen) catch unreachable;
                @memcpy(fcopy, fname[0..flen]);

                facets_to_process.append(allocator, .{ .id = c.DatumGetInt32(fid_datum), .name = fcopy }) catch unreachable;
            }
        }
    }

    // Process each facet
    for (facets_to_process.items) |f| {
        var counts = std.ArrayList(FacetCount).empty;
        defer counts.deinit(allocator);

        const values_query = std.fmt.allocPrintSentinel(allocator, "SELECT DISTINCT facet_value FROM \"{s}\".\"{s}\" WHERE facet_id = {d} ORDER BY facet_value", .{ schema_name, facets_table, f.id }, 0) catch unreachable;
        if (c.SPI_execute(values_query.ptr, true, 0) != c.SPI_OK_SELECT) continue;

        const values_proc = c.SPI_processed;
        var v_idx: u64 = 0;
        while (v_idx < values_proc) : (v_idx += 1) {
            const value_tuple = c.SPI_tuptable.*.vals[v_idx];
            const value_desc = c.SPI_tuptable.*.tupdesc;

            var isnull_val: bool = false;
            const val_datum = c.SPI_getbinval(value_tuple, value_desc, 1, &isnull_val);
            if (isnull_val) continue;

            const v_text = c.TextDatumGetCString(val_datum);
            const v_len = std.mem.len(v_text);
            const v_copy = allocator.alloc(u8, v_len) catch unreachable;
            @memcpy(v_copy, v_text[0..v_len]);

            const postings_query = std.fmt.allocPrintSentinel(allocator, "SELECT chunk_id, postinglist FROM \"{s}\".\"{s}\" WHERE facet_id = {d} AND facet_value = $1 ORDER BY chunk_id", .{ schema_name, facets_table, f.id }, 0) catch unreachable;
            var argtypes = [_]c.Oid{c.TEXTOID};
            var values = [_]c.Datum{c.PointerGetDatum(c.cstring_to_text_with_len(v_text, @intCast(v_len)))};
            var nulls = [_]u8{' '};

            if (c.SPI_execute_with_args(postings_query.ptr, 1, &argtypes, &values, &nulls, true, 0) != c.SPI_OK_SELECT) {
                allocator.free(v_copy);
                continue;
            }

            var facet_bitmap: ?*c.roaring_bitmap_t = null;
            const proc = c.SPI_processed;
            var k: u64 = 0;
            while (k < proc) : (k += 1) {
                const tuple = c.SPI_tuptable.*.vals[k];
                const desc = c.SPI_tuptable.*.tupdesc;

                var isnull_chunk_id: bool = false;
                const chunk_id_datum = c.SPI_getbinval(tuple, desc, 1, &isnull_chunk_id);
                if (isnull_chunk_id) continue;
                const chunk_id: u32 = @intCast(c.DatumGetInt32(chunk_id_datum));

                var isnull_p: bool = false;
                const p_datum = c.SPI_getbinval(tuple, desc, 2, &isnull_p);
                if (isnull_p) continue;

                const p_ptr = utils.detoast_datum(p_datum);
                const p_len = utils.varsize(p_ptr) - utils.varhdrsz();
                const p_data = utils.vardata(p_ptr);

                const bm = c.roaring_bitmap_portable_deserialize_safe(p_data, @intCast(p_len));
                defer c.roaring_bitmap_free(bm);

                var pl_iter: c.roaring_uint32_iterator_t = undefined;
                c.roaring_iterator_init(bm, &pl_iter);
                while (pl_iter.has_value) {
                    const original_id: u32 = (chunk_id << chunk_bits) | pl_iter.current_value;

                    if (facet_bitmap == null) {
                        facet_bitmap = c.roaring_bitmap_create();
                    }
                    c.roaring_bitmap_add(facet_bitmap, original_id);
                    _ = c.roaring_uint32_iterator_advance(&pl_iter);
                }
            }

            if (facet_bitmap == null) {
                allocator.free(v_copy);
                continue;
            }

            const cardinality = if (filter_bitmap) |filter| c.roaring_bitmap_and_cardinality(facet_bitmap.?, filter) else c.roaring_bitmap_get_cardinality(facet_bitmap.?);
            c.roaring_bitmap_free(facet_bitmap.?);

            if (cardinality > 0) {
                counts.append(allocator, .{ .facet_name = f.name, .facet_value = v_copy, .cardinality = cardinality, .facet_id = f.id }) catch unreachable;
            } else {
                allocator.free(v_copy);
            }
        }

        const Sorter = struct {
            fn lessThan(_: void, lhs: FacetCount, rhs: FacetCount) bool {
                if (lhs.cardinality != rhs.cardinality) return lhs.cardinality > rhs.cardinality;
                return std.mem.order(u8, lhs.facet_value, rhs.facet_value) == .lt;
            }
        };
        std.sort.block(FacetCount, counts.items, {}, Sorter.lessThan);

        var count: usize = 0;
        for (counts.items) |item| {
            if (count >= limit) break;

            var values = [_]c.Datum{ c.PointerGetDatum(c.cstring_to_text_with_len(item.facet_name.ptr, @intCast(item.facet_name.len))), c.PointerGetDatum(c.cstring_to_text_with_len(item.facet_value.ptr, @intCast(item.facet_value.len))), c.Int64GetDatum(@intCast(item.cardinality)), c.Int32GetDatum(item.facet_id) };
            var nulls = [_]bool{ false, false, false, false };

            c.tuplestore_putvalues(rsi.setResult, rsi.setDesc, &values, &nulls);
            count += 1;
        }
    }

    return 0;
}
