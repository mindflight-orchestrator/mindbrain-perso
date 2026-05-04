const std = @import("std");

pub const Error = error{
    InvalidEmbeddingBlob,
};

pub fn encodeF32Le(allocator: std.mem.Allocator, values: []const f32) ![]u8 {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(f32));
    for (values, 0..) |value, index| {
        const start = index * @sizeOf(f32);
        const chunk: *[4]u8 = @ptrCast(bytes[start..][0..4]);
        std.mem.writeInt(u32, chunk, @bitCast(value), .little);
    }
    return bytes;
}

pub fn decodeF32Le(allocator: std.mem.Allocator, blob: []const u8, dimensions: usize) ![]f32 {
    if (blob.len != dimensions * @sizeOf(f32)) return error.InvalidEmbeddingBlob;

    const values = try allocator.alloc(f32, dimensions);
    for (values, 0..) |*value, index| {
        const start = index * @sizeOf(f32);
        const chunk: *const [4]u8 = @ptrCast(blob[start..][0..4]);
        const bits = std.mem.readInt(u32, chunk, .little);
        value.* = @bitCast(bits);
    }
    return values;
}

test "vector blob round-trips little endian f32 values" {
    const input = [_]f32{ 1.0, -2.5, 0.125, 42.0 };
    const bytes = try encodeF32Le(std.testing.allocator, &input);
    defer std.testing.allocator.free(bytes);

    const output = try decodeF32Le(std.testing.allocator, bytes, input.len);
    defer std.testing.allocator.free(output);

    try std.testing.expectEqualSlices(f32, &input, output);
}

test "vector blob rejects mismatched dimensions" {
    const input = [_]f32{ 1.0, 2.0 };
    const bytes = try encodeF32Le(std.testing.allocator, &input);
    defer std.testing.allocator.free(bytes);

    try std.testing.expectError(
        error.InvalidEmbeddingBlob,
        decodeF32Le(std.testing.allocator, bytes, input.len + 1),
    );
}

test "vector blob encoding is little endian independent of native layout" {
    const input = [_]f32{1.0};
    const bytes = try encodeF32Le(std.testing.allocator, &input);
    defer std.testing.allocator.free(bytes);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00, 0x80, 0x3f }, bytes);
}
