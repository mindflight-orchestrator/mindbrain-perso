const std = @import("std");

pub const c = @cImport({
    @cInclude("roaring.h");
});

/// Dense runtime IDs stay in u32 so they map directly onto CRoaring.
pub const DenseId = u32;
pub const EntityId = DenseId;
pub const RelationId = DenseId;
pub const ChunkId = DenseId;

pub const Error = error{
    OutOfMemory,
    DeserializeFailed,
    SerializeFailed,
};

pub const Bitmap = struct {
    handle: *c.roaring_bitmap_t,

    pub fn empty() Error!Bitmap {
        const handle = c.roaring_bitmap_create() orelse return error.OutOfMemory;
        return .{ .handle = handle };
    }

    pub fn fromSlice(values: []const DenseId) Error!Bitmap {
        var bitmap = try empty();
        errdefer bitmap.deinit();

        for (values) |value| {
            c.roaring_bitmap_add(bitmap.handle, value);
        }

        return bitmap;
    }

    pub fn deserializePortable(bytes: []const u8) Error!Bitmap {
        const handle = c.roaring_bitmap_portable_deserialize_safe(
            bytes.ptr,
            @intCast(bytes.len),
        ) orelse return error.DeserializeFailed;

        return .{ .handle = handle };
    }

    pub fn clone(self: Bitmap) Error!Bitmap {
        const handle = c.roaring_bitmap_copy(self.handle) orelse return error.OutOfMemory;
        return .{ .handle = handle };
    }

    pub fn deinit(self: *Bitmap) void {
        c.roaring_bitmap_free(self.handle);
        self.* = undefined;
    }

    pub fn cardinality(self: Bitmap) u64 {
        return c.roaring_bitmap_get_cardinality(self.handle);
    }

    pub fn isEmpty(self: Bitmap) bool {
        return c.roaring_bitmap_is_empty(self.handle);
    }

    pub fn contains(self: Bitmap, value: DenseId) bool {
        return c.roaring_bitmap_contains(self.handle, value);
    }

    pub fn add(self: *Bitmap, value: DenseId) void {
        c.roaring_bitmap_add(self.handle, value);
    }

    pub fn remove(self: *Bitmap, value: DenseId) void {
        c.roaring_bitmap_remove(self.handle, value);
    }

    pub fn andInPlace(self: *Bitmap, other: Bitmap) void {
        _ = c.roaring_bitmap_and_inplace(self.handle, other.handle);
    }

    pub fn orInPlace(self: *Bitmap, other: Bitmap) void {
        _ = c.roaring_bitmap_or_inplace(self.handle, other.handle);
    }

    pub fn andNew(self: Bitmap, other: Bitmap) Error!Bitmap {
        const handle = c.roaring_bitmap_and(self.handle, other.handle) orelse return error.OutOfMemory;
        return .{ .handle = handle };
    }

    pub fn orNew(self: Bitmap, other: Bitmap) Error!Bitmap {
        const handle = c.roaring_bitmap_or(self.handle, other.handle) orelse return error.OutOfMemory;
        return .{ .handle = handle };
    }

    pub fn andNotNew(self: Bitmap, other: Bitmap) Error!Bitmap {
        const handle = c.roaring_bitmap_andnot(self.handle, other.handle) orelse return error.OutOfMemory;
        return .{ .handle = handle };
    }

    pub fn runOptimize(self: *Bitmap) bool {
        return c.roaring_bitmap_run_optimize(self.handle);
    }

    pub fn shrinkToFit(self: *Bitmap) void {
        _ = c.roaring_bitmap_shrink_to_fit(self.handle);
    }

    /// Apply the storage-oriented compaction passes that are safe for
    /// long-lived or persisted bitmaps.
    pub fn optimizeForStorage(self: *Bitmap) void {
        _ = self.runOptimize();
        self.shrinkToFit();
    }

    pub fn serializePortable(self: Bitmap, allocator: std.mem.Allocator) Error![]u8 {
        const size: usize = @intCast(c.roaring_bitmap_portable_size_in_bytes(self.handle));
        const buf = try allocator.alloc(u8, size);
        errdefer allocator.free(buf);

        const written = c.roaring_bitmap_portable_serialize(self.handle, buf.ptr);
        if (written != size) return error.SerializeFailed;

        return buf;
    }

    /// Persist using the portable CRoaring format after storage-oriented
    /// compaction without mutating the caller's bitmap instance.
    pub fn serializePortableStable(self: Bitmap, allocator: std.mem.Allocator) Error![]u8 {
        var optimized = try self.clone();
        defer optimized.deinit();
        optimized.optimizeForStorage();
        return optimized.serializePortable(allocator);
    }

    pub fn toArray(self: Bitmap, allocator: std.mem.Allocator) Error![]DenseId {
        const count: usize = @intCast(self.cardinality());
        const values = try allocator.alloc(DenseId, count);
        c.roaring_bitmap_to_uint32_array(self.handle, values.ptr);
        return values;
    }
};
