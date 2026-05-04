const std = @import("std");
const interfaces = @import("interfaces.zig");

pub const Score = struct {
    distance: f64,
    similarity: f64,
};

pub fn score(metric: interfaces.VectorDistanceMetric, query: []const f32, candidate: []const f32) Score {
    return switch (metric) {
        .cosine => cosineScore(query, candidate),
        .l2 => l2Score(query, candidate),
        .inner_product => innerProductScore(query, candidate),
    };
}

pub fn lessThan(_: void, lhs: interfaces.VectorSearchMatch, rhs: interfaces.VectorSearchMatch) bool {
    if (lhs.similarity == rhs.similarity) return lhs.doc_id < rhs.doc_id;
    return lhs.similarity > rhs.similarity;
}

fn cosineScore(a: []const f32, b: []const f32) Score {
    const similarity = cosineSimilarity(a, b);
    return .{
        .distance = 1.0 - similarity,
        .similarity = similarity,
    };
}

fn l2Score(a: []const f32, b: []const f32) Score {
    const distance = @sqrt(squaredL2(a, b));
    return .{
        .distance = distance,
        .similarity = 1.0 / (1.0 + distance),
    };
}

fn innerProductScore(a: []const f32, b: []const f32) Score {
    const similarity = dotProduct(a, b);
    return .{
        .distance = -similarity,
        .similarity = similarity,
    };
}

fn cosineSimilarity(a: []const f32, b: []const f32) f64 {
    var dot: f64 = 0.0;
    var norm_a: f64 = 0.0;
    var norm_b: f64 = 0.0;
    const len = @min(a.len, b.len);
    for (0..len) |i| {
        const av = @as(f64, a[i]);
        const bv = @as(f64, b[i]);
        dot += av * bv;
        norm_a += av * av;
        norm_b += bv * bv;
    }
    if (norm_a == 0.0 or norm_b == 0.0) return 0.0;
    return dot / (@sqrt(norm_a) * @sqrt(norm_b));
}

fn squaredL2(a: []const f32, b: []const f32) f64 {
    var sum: f64 = 0.0;
    const len = @min(a.len, b.len);
    for (0..len) |i| {
        const delta = @as(f64, a[i]) - @as(f64, b[i]);
        sum += delta * delta;
    }
    return sum;
}

fn dotProduct(a: []const f32, b: []const f32) f64 {
    var dot: f64 = 0.0;
    const len = @min(a.len, b.len);
    for (0..len) |i| {
        dot += @as(f64, a[i]) * @as(f64, b[i]);
    }
    return dot;
}

test "vector distance ranks cosine neighbors by similarity" {
    const query = [_]f32{ 1.0, 0.0 };
    const close = score(.cosine, &query, &.{ 1.0, 0.0 });
    const far = score(.cosine, &query, &.{ 0.0, 1.0 });

    try std.testing.expect(close.similarity > far.similarity);
    try std.testing.expect(close.distance < far.distance);
}

test "vector distance supports l2 and inner product metrics" {
    const query = [_]f32{ 1.0, 0.0 };
    const l2_close = score(.l2, &query, &.{ 1.0, 0.0 });
    const l2_far = score(.l2, &query, &.{ 3.0, 0.0 });
    try std.testing.expect(l2_close.similarity > l2_far.similarity);

    const ip_close = score(.inner_product, &query, &.{ 2.0, 0.0 });
    const ip_far = score(.inner_product, &query, &.{ 0.5, 0.0 });
    try std.testing.expect(ip_close.similarity > ip_far.similarity);
}
