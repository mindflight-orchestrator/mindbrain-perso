const utils = @import("utils.zig");
const graph_traversal = @import("graph_traversal.zig");
const shortest_path = @import("shortest_path.zig");

const c = utils.c;

const PgFinfoRecord = utils.PgFinfoRecord;

// k_hops_filtered_native
export fn pg_finfo_k_hops_filtered_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

export fn k_hops_filtered_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return graph_traversal.k_hops_filtered_wrapper(fcinfo);
}

// shortest_path_filtered_native
export fn pg_finfo_shortest_path_filtered_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

export fn shortest_path_filtered_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    return shortest_path.shortest_path_filtered_wrapper(fcinfo);
}
