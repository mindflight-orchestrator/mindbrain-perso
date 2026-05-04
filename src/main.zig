const facets_mod = @import("mb_facets/main.zig");
const graph_mod = @import("mb_graph/main.zig");
const ontology_mod = @import("mb_ontology/main.zig");
const pragma_mod = @import("mb_pragma/main.zig");
const facets_utils = @import("mb_facets/utils.zig");

const c = facets_utils.c;

comptime {
    _ = facets_mod;
    _ = graph_mod;
    _ = ontology_mod;
    _ = pragma_mod;
}

const pg_magic_data = c.Pg_magic_struct{
    .len = @sizeOf(c.Pg_magic_struct),
    .version = 1700,
    .funcmaxargs = 100,
    .indexmaxkeys = 32,
    .namedatalen = 64,
    .float8byval = 1,
    .abi_extra = "PostgreSQL".* ++ [_]u8{0} ** 22,
};

export fn Pg_magic_func() callconv(.c) *const c.Pg_magic_struct {
    return &pg_magic_data;
}
