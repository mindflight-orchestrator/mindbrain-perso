const std = @import("std");

pub const c = @cImport({
    @cDefine("PG_MODULE_MAGIC", "1");
    @cDefine("VARHDRSZ_EXTERNAL", "VARHDRSZ");
    @cInclude("postgres.h");
    @cInclude("fmgr.h");
    @cInclude("funcapi.h");
    @cInclude("utils/builtins.h");
    @cInclude("utils/elog.h");
    @cInclude("utils/tuplestore.h");
});

extern fn fcinfo_get_arg_value_helper(fcinfo: c.FunctionCallInfo, n: c_int) c.Datum;
extern fn fcinfo_get_arg_isnull_helper(fcinfo: c.FunctionCallInfo, n: c_int) bool;
extern fn fcinfo_set_isnull_helper(fcinfo: c.FunctionCallInfo, isnull: bool) void;
extern fn elog_helper(level: c_int, msg: [*c]const u8) void;
extern fn srf_return_empty_helper(fcinfo: c.FunctionCallInfo) c.Datum;
extern fn text_datum_from_buf(buf: [*c]const u8, len: usize) c.Datum;
extern fn text_datum_to_slice(d: c.Datum, out_ptr: *[*c]const u8, out_len: *usize) void;

pub const PgFinfoRecord = extern struct {
    api_version: c_int,
};

pub fn get_arg_datum(fcinfo: c.FunctionCallInfo, n: usize) c.Datum {
    return fcinfo_get_arg_value_helper(fcinfo, @intCast(n));
}

pub fn is_arg_null(fcinfo: c.FunctionCallInfo, n: usize) bool {
    return fcinfo_get_arg_isnull_helper(fcinfo, @intCast(n));
}

pub fn set_return_null(fcinfo: c.FunctionCallInfo) void {
    fcinfo_set_isnull_helper(fcinfo, true);
}

pub fn set_return_not_null(fcinfo: c.FunctionCallInfo) void {
    fcinfo_set_isnull_helper(fcinfo, false);
}

pub fn srf_return_empty(fcinfo: c.FunctionCallInfo) c.Datum {
    return srf_return_empty_helper(fcinfo);
}

pub fn textDatumFromSlice(slice: []const u8) c.Datum {
    return text_datum_from_buf(slice.ptr, slice.len);
}

pub fn textDatumToSlice(d: c.Datum) []const u8 {
    var ptr: [*c]const u8 = undefined;
    var len: usize = undefined;
    text_datum_to_slice(d, &ptr, &len);
    return ptr[0..len];
}

/// PostgreSQL palloc-based allocator for use in PG function context.
pub const PgAllocator = struct {
    pub fn allocator() std.mem.Allocator {
        return std.mem.Allocator{
            .ptr = undefined,
            .vtable = &vtable,
        };
    }
    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .free = free,
        .remap = remap,
    };
    fn alloc(_: *anyopaque, len: usize, _: std.mem.Alignment, _: usize) ?[*]u8 {
        const ptr = c.palloc(len);
        if (ptr == null) return null;
        return @ptrCast(ptr);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(_: *anyopaque, buf: []u8, _: std.mem.Alignment, _: usize) void {
        c.pfree(buf.ptr);
    }
};
