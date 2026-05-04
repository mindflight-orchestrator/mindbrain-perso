const utils = @import("utils.zig");
const dsl_parser = @import("dsl_parser.zig");
const c = utils.c;

const PgFinfoRecord = utils.PgFinfoRecord;

// pragma_rank_native - Zig native scorer (stub: returns empty set); also exposed as pragma_rank_zig for SQL benchmarks.
export fn pg_finfo_pragma_rank_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

export fn pg_finfo_pragma_rank_zig() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

fn pragmaRankStubImpl(fcinfo: c.FunctionCallInfo) c.Datum {
    return utils.srf_return_empty(fcinfo);
}

export fn pragma_rank_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return pragmaRankStubImpl(fcinfo);
}

export fn pragma_rank_zig(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return pragmaRankStubImpl(fcinfo);
}

// pragma_next_hops_native - Zig next-hop helper (stub: returns empty set); also exposed as pragma_next_hops_zig.
export fn pg_finfo_pragma_next_hops_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

export fn pg_finfo_pragma_next_hops_zig() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

fn pragmaNextHopsStubImpl(fcinfo: c.FunctionCallInfo) c.Datum {
    return utils.srf_return_empty(fcinfo);
}

export fn pragma_next_hops_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return pragmaNextHopsStubImpl(fcinfo);
}

export fn pragma_next_hops_zig(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return pragmaNextHopsStubImpl(fcinfo);
}

// pragma_parse_proposition_line: parses one DSL line, returns JSON text (cast to jsonb in SQL)
export fn pg_finfo_pragma_parse_proposition_line() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

export fn pragma_parse_proposition_line(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.set_return_null(fcinfo);
        return 0;
    }
    const line_datum = utils.get_arg_datum(fcinfo, 0);
    const line_slice = utils.textDatumToSlice(line_datum);

    const allocator = utils.PgAllocator.allocator();
    var rec = dsl_parser.parseLine(allocator, line_slice);
    defer if (rec) |*r| r.deinit(allocator);

    if (rec) |*r| {
        const json_str = r.toJson(allocator) catch {
            utils.set_return_null(fcinfo);
            return 0;
        };
        defer allocator.free(json_str);
        return utils.textDatumFromSlice(json_str);
    }
    // Return "null" for unparseable lines
    return utils.textDatumFromSlice("null");
}
