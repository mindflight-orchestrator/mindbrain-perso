const std = @import("std");

pub const c = @cImport({
    @cDefine("PG_MODULE_MAGIC", "1");
    @cDefine("VARHDRSZ_EXTERNAL", "VARHDRSZ");
    @cInclude("postgres.h");
    @cInclude("fmgr.h");
    @cInclude("executor/spi.h");
    @cInclude("utils/builtins.h");
    @cInclude("utils/elog.h");
    @cInclude("utils/memutils.h");
    @cInclude("utils/array.h");
    @cInclude("catalog/pg_type.h");
    @cInclude("roaringbitmap.h");
    @cInclude("roaring.h");
});

// C helper externs (from helper.c)
extern fn fcinfo_get_arg_value_helper(fcinfo: c.FunctionCallInfo, n: c_int) c.Datum;
extern fn fcinfo_get_arg_isnull_helper(fcinfo: c.FunctionCallInfo, n: c_int) bool;
extern fn fcinfo_set_isnull_helper(fcinfo: c.FunctionCallInfo, isnull: bool) void;
extern fn elog_helper(level: c_int, msg: [*c]const u8) void;
extern fn detoast_datum_helper(d: c.Datum) [*c]c.struct_varlena;
extern fn varhdrsz_helper() c_int;
extern fn varsize_any_exhdr_helper(ptr: [*c]c.struct_varlena) usize;
extern fn vardata_any_helper(ptr: [*c]c.struct_varlena) ?*anyopaque;
extern fn set_varsize_helper(ptr: [*c]c.struct_varlena, size: c_int) void;
extern fn pointer_get_datum_helper(ptr: ?*const anyopaque) c.Datum;
extern fn datum_get_pointer_helper(d: c.Datum) ?*const anyopaque;

// Native function info record for V1 calling convention
pub const PgFinfoRecord = extern struct {
    api_version: c_int,
};

// VARHDRSZ is sizeof(int32) = 4, always
pub const VARHDRSZ: usize = 4;

// palloc-based allocator compatible with Zig 0.15 std.mem.Allocator
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

    fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
        // palloc guarantees only MAXALIGN (8 bytes); refuse stricter requests
        // instead of handing back misaligned memory.
        if (alignment.toByteUnits() > 8) return null;
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

pub fn elog(level: c_int, msg: []const u8) void {
    const c_msg = c.palloc(msg.len + 1);
    @memcpy(@as([*]u8, @ptrCast(c_msg))[0..msg.len], msg);
    @as([*]u8, @ptrCast(c_msg))[msg.len] = 0;
    elog_helper(level, @as([*c]const u8, @ptrCast(c_msg)));
}

pub fn elogFmt(level: c_int, comptime fmt: []const u8, args: anytype) void {
    var buf: [512]u8 = undefined;
    const msg = std.fmt.bufPrintZ(&buf, fmt, args) catch {
        elog(level, "Error message formatting failed");
        return;
    };
    elog(level, msg);
}

pub fn detoast_datum(d: c.Datum) [*c]c.struct_varlena {
    return detoast_datum_helper(d);
}

/// VARSIZE_ANY_EXHDR: payload length regardless of 1-byte or 4-byte header.
pub fn varsize_any_exhdr(ptr: [*c]c.struct_varlena) usize {
    return varsize_any_exhdr_helper(ptr);
}

/// VARDATA_ANY: payload pointer regardless of 1-byte or 4-byte header.
pub fn vardata_any(ptr: [*c]c.struct_varlena) [*]u8 {
    return @ptrCast(vardata_any_helper(ptr).?);
}

/// DatumGetInt32 semantics: truncate the Datum to its low 32 bits and
/// reinterpret as signed. A plain @intCast aborts the backend whenever the
/// int4 argument is negative (sign-extended through the Datum).
pub fn datum_get_int32(d: c.Datum) i32 {
    return @bitCast(@as(u32, @truncate(d)));
}

pub fn pointer_get_datum(ptr: anytype) c.Datum {
    return pointer_get_datum_helper(@ptrCast(@alignCast(ptr)));
}

pub fn datum_get_pointer(d: c.Datum) ?*const anyopaque {
    return datum_get_pointer_helper(d);
}

pub fn get_arg_datum(fcinfo: c.FunctionCallInfo, n: usize) c.Datum {
    return fcinfo_get_arg_value_helper(fcinfo, @intCast(n));
}

pub fn is_arg_null(fcinfo: c.FunctionCallInfo, n: usize) bool {
    return fcinfo_get_arg_isnull_helper(fcinfo, @intCast(n));
}

pub fn set_return_null(fcinfo: c.FunctionCallInfo) void {
    fcinfo_set_isnull_helper(fcinfo, true);
}

// Look up the OID of the roaringbitmap type. Assumes SPI is connected.
// Raises an error when the type is missing: silently falling back to bytea
// made every downstream query fail or return wrong "no edges" answers.
pub fn getRoaringBitmapOid() c.Oid {
    const ret = c.SPI_execute("SELECT oid FROM pg_type WHERE typname = 'roaringbitmap'", true, 1);
    if (ret == c.SPI_OK_SELECT and c.SPI_processed > 0) {
        var isnull: bool = false;
        const datum = c.SPI_getbinval(
            c.SPI_tuptable.*.vals[0],
            c.SPI_tuptable.*.tupdesc,
            1,
            &isnull,
        );
        if (!isnull) return @as(c.Oid, @truncate(datum));
    }
    elog(c.ERROR, "pg_dgraph: type 'roaringbitmap' not found; install the roaringbitmap extension");
    unreachable; // elog(ERROR) longjmps and never returns
}
