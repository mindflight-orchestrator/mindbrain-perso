const std = @import("std");
const utils = @import("utils.zig");
const c = utils.c;

/// Deserialize a roaringbitmap datum into a CRoaring bitmap.
/// The returned bitmap is heap-allocated by CRoaring and must be freed
/// with c.roaring_bitmap_free(). Calls elog(ERROR) on failure (never returns).
pub fn datumToRoaringBitmap(datum: c.Datum) *c.roaring_bitmap_t {
    const varlena = utils.detoast_datum(datum);
    const varlena_ptr = @as(*align(1) c.struct_varlena, @ptrCast(varlena));

    // Read 4-byte varlena header: size is stored as (total_bytes << 2)
    const header_bytes = @as(*align(1) [4]u8, @ptrCast(varlena_ptr));
    const header_u32 = std.mem.readInt(u32, header_bytes, .little);
    const total_size = (header_u32 >> 2) & 0x3FFFFFFF;
    const len = @as(usize, @intCast(total_size)) - utils.VARHDRSZ;
    const data = @as([*]u8, @ptrCast(varlena_ptr)) + utils.VARHDRSZ;

    const bitmap = c.roaring_bitmap_portable_deserialize_safe(data, @intCast(len));
    if (bitmap == null) {
        utils.elog(c.ERROR, "pg_dgraph: failed to deserialize roaring bitmap");
        unreachable; // elog(ERROR) does a longjmp and never returns
    }
    return bitmap.?;
}

/// Serialize a CRoaring bitmap into a roaringbitmap datum (palloc'd bytea).
/// Calls elog(ERROR) on allocation failure (never returns).
pub fn roaringBitmapToDatum(bitmap: *c.roaring_bitmap_t) c.Datum {
    const size = c.roaring_bitmap_portable_size_in_bytes(bitmap);
    const total_size = utils.VARHDRSZ + @as(usize, @intCast(size));

    const allocator = utils.PgAllocator.allocator();
    const bytea = allocator.alloc(u8, total_size) catch {
        utils.elog(c.ERROR, "pg_dgraph: out of memory allocating roaring bitmap datum");
        unreachable;
    };

    // Write 4-byte varlena header
    const header_value: u32 = @intCast(total_size << 2);
    std.mem.writeInt(u32, bytea[0..4], header_value, .little);

    const written = c.roaring_bitmap_portable_serialize(
        bitmap,
        bytea.ptr + utils.VARHDRSZ,
    );
    if (written != size) {
        utils.elog(c.ERROR, "pg_dgraph: bitmap serialization size mismatch");
        unreachable;
    }

    return utils.pointer_get_datum(bytea.ptr);
}

pub fn createEmptyBitmap() *c.roaring_bitmap_t {
    return c.roaring_bitmap_create();
}

pub fn createBitmapFromArray(values: []const i32) *c.roaring_bitmap_t {
    const bitmap = c.roaring_bitmap_create();
    for (values) |val| {
        c.roaring_bitmap_add(bitmap, @intCast(val));
    }
    return bitmap;
}

pub fn bitmapToArray(allocator: std.mem.Allocator, bitmap: *c.roaring_bitmap_t) ![]i32 {
    const cardinality = c.roaring_bitmap_get_cardinality(bitmap);
    const array = try allocator.alloc(i32, @intCast(cardinality));
    c.roaring_bitmap_to_uint32_array(bitmap, @ptrCast(array));
    return array;
}

pub fn isBitmapEmpty(bitmap: *c.roaring_bitmap_t) bool {
    return c.roaring_bitmap_is_empty(bitmap);
}

pub fn bitmapCardinality(bitmap: *c.roaring_bitmap_t) u64 {
    return c.roaring_bitmap_get_cardinality(bitmap);
}

pub fn bitmapContains(bitmap: *c.roaring_bitmap_t, value: i32) bool {
    return c.roaring_bitmap_contains(bitmap, @intCast(value)) != 0;
}

pub fn bitmapOr(a: *c.roaring_bitmap_t, b: *c.roaring_bitmap_t) *c.roaring_bitmap_t {
    return c.roaring_bitmap_or(a, b);
}

pub fn bitmapAnd(a: *c.roaring_bitmap_t, b: *c.roaring_bitmap_t) *c.roaring_bitmap_t {
    return c.roaring_bitmap_and(a, b);
}

pub fn bitmapDifference(a: *c.roaring_bitmap_t, b: *c.roaring_bitmap_t) *c.roaring_bitmap_t {
    return c.roaring_bitmap_andnot(a, b);
}

pub fn bitmapOrInplace(a: *c.roaring_bitmap_t, b: *const c.roaring_bitmap_t) void {
    _ = c.roaring_bitmap_or_inplace(a, @constCast(b));
}

pub fn bitmapAndInplace(a: *const c.roaring_bitmap_t, b: *const c.roaring_bitmap_t) void {
    _ = c.roaring_bitmap_and_inplace(@constCast(a), @constCast(b));
}

/// Compress run-length sequences in-place.
/// Returns true if the bitmap was modified (i.e. compression helped).
/// Call after accumulating many OR operations to reduce serialisation cost.
pub fn bitmapRunOptimize(bitmap: *c.roaring_bitmap_t) bool {
    return c.roaring_bitmap_run_optimize(bitmap);
}

/// Shrink internal storage to minimum required.
/// Useful after large deletions or before long-lived storage.
pub fn bitmapShrinkToFit(bitmap: *c.roaring_bitmap_t) void {
    _ = c.roaring_bitmap_shrink_to_fit(bitmap);
}
