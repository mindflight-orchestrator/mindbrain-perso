const std = @import("std");
const zig16_compat = @import("zig16_compat.zig");

pub const default_budget_json_path = "fixtures/llm_context_budgets.json";

pub const BudgetSpec = struct {
    context_tokens: u64,
    output_tokens: u64,
    prompt_reserve_tokens: u64,
    chars_per_token: u32,
};

pub const PromptBudgetReport = struct {
    provider: []const u8,
    model: []const u8,
    input_budget_chars: usize,
    system_chars: usize,
    fixed_user_chars: usize,
    per_doc_limits: []usize,
    truncated_doc_ids: []u64,
    needs_batch: bool,
    batch_doc_ids: []const []const u64,

    pub fn deinit(self: *PromptBudgetReport, allocator: std.mem.Allocator) void {
        allocator.free(self.per_doc_limits);
        allocator.free(self.truncated_doc_ids);
        for (self.batch_doc_ids) |batch| {
            allocator.free(batch);
        }
        allocator.free(self.batch_doc_ids);
        self.* = undefined;
    }
};

const MatchKind = enum { exact, prefix };

const ModelEntry = struct {
    provider: ?[]const u8,
    model: ?[]const u8,
    match: MatchKind,
    spec: BudgetSpec,
};

const BudgetFile = struct {
    default: BudgetSpec,
    models: []const ModelEntry,

    /// Frees a file produced by `loadBudgetFromJson`. Must not be called on
    /// `builtin_budget_file`, whose entries are static.
    pub fn deinit(self: BudgetFile, allocator: std.mem.Allocator) void {
        for (self.models) |entry| {
            if (entry.provider) |provider| allocator.free(provider);
            if (entry.model) |model| allocator.free(model);
        }
        allocator.free(self.models);
    }
};

pub fn estimateTokensFromChars(chars: usize, chars_per_token: u32) usize {
    const divisor: usize = if (chars_per_token == 0) 4 else @intCast(chars_per_token);
    return (chars + divisor - 1) / divisor;
}

pub fn inputBudgetChars(spec: BudgetSpec) usize {
    const available_tokens = spec.context_tokens -| spec.output_tokens -| spec.prompt_reserve_tokens;
    const chars: u64 = available_tokens * @as(u64, spec.chars_per_token);
    return std.math.cast(usize, chars) orelse std.math.maxInt(usize);
}

pub fn loadBudgetFromJson(allocator: std.mem.Allocator, json: []const u8) !BudgetFile {
    var parsed = try std.json.parseFromSlice(
        struct {
            default: BudgetSpec,
            models: []struct {
                provider: ?[]const u8 = null,
                model: ?[]const u8 = null,
                match: ?[]const u8 = null,
                context_tokens: u64,
                output_tokens: u64,
                prompt_reserve_tokens: u64,
                chars_per_token: u32 = 4,
            },
        },
        allocator,
        json,
        .{ .ignore_unknown_fields = true },
    );
    defer parsed.deinit();

    var models = try allocator.alloc(ModelEntry, parsed.value.models.len);
    var filled: usize = 0;
    errdefer {
        for (models[0..filled]) |entry| {
            if (entry.provider) |provider| allocator.free(provider);
            if (entry.model) |model| allocator.free(model);
        }
        allocator.free(models);
    }
    for (parsed.value.models) |row| {
        const match: MatchKind = if (row.match) |m| blk: {
            if (std.mem.eql(u8, m, "prefix")) break :blk .prefix;
            break :blk .exact;
        } else .exact;
        // Dupe out of the parser arena: `parsed` is freed before the entries
        // are used, so borrowed slices (notably escaped strings, which always
        // live in the arena) would dangle.
        const provider = if (row.provider) |p| try allocator.dupe(u8, p) else null;
        errdefer if (provider) |p| allocator.free(p);
        const model = if (row.model) |m| try allocator.dupe(u8, m) else null;
        models[filled] = .{
            .provider = provider,
            .model = model,
            .match = match,
            .spec = .{
                .context_tokens = row.context_tokens,
                .output_tokens = row.output_tokens,
                .prompt_reserve_tokens = row.prompt_reserve_tokens,
                .chars_per_token = row.chars_per_token,
            },
        };
        filled += 1;
    }
    return .{ .default = parsed.value.default, .models = models };
}

pub fn resolveBudgetSpec(file: BudgetFile, provider: []const u8, model: []const u8) BudgetSpec {
    var prefix_match: ?BudgetSpec = null;
    for (file.models) |entry| {
        if (entry.provider) |p| {
            if (!std.mem.eql(u8, p, provider)) continue;
        }
        if (entry.model) |pattern| {
            switch (entry.match) {
                .exact => {
                    if (std.mem.eql(u8, pattern, model)) return entry.spec;
                },
                .prefix => {
                    const prefix = if (std.mem.endsWith(u8, pattern, "*"))
                        pattern[0 .. pattern.len - 1]
                    else
                        pattern;
                    if (prefix.len > 0 and std.mem.startsWith(u8, model, prefix)) {
                        prefix_match = entry.spec;
                    }
                },
            }
        }
    }
    if (prefix_match) |spec| return spec;
    return file.default;
}

const builtin_budget_file: BudgetFile = .{
    .default = .{
        .context_tokens = 128000,
        .output_tokens = 16384,
        .prompt_reserve_tokens = 8192,
        .chars_per_token = 4,
    },
    .models = &.{
        .{
            .provider = "openai",
            .model = "gpt-5-mini",
            .match = .exact,
            .spec = .{
                .context_tokens = 400000,
                .output_tokens = 128000,
                .prompt_reserve_tokens = 32000,
                .chars_per_token = 4,
            },
        },
        .{
            .provider = "openai",
            .model = "gpt-5-nano",
            .match = .exact,
            .spec = .{
                .context_tokens = 128000,
                .output_tokens = 32768,
                .prompt_reserve_tokens = 16384,
                .chars_per_token = 4,
            },
        },
        .{
            .provider = "openai",
            .model = "gpt-5*",
            .match = .prefix,
            .spec = .{
                .context_tokens = 400000,
                .output_tokens = 128000,
                .prompt_reserve_tokens = 32000,
                .chars_per_token = 4,
            },
        },
    },
};

pub fn loadBudget(
    allocator: std.mem.Allocator,
    provider: []const u8,
    model: []const u8,
    json_path: ?[]const u8,
) !BudgetSpec {
    if (json_path) |path| {
        const json = try std.Io.Dir.cwd().readFileAlloc(zig16_compat.io(), path, allocator, .limited(1024 * 1024));
        defer allocator.free(json);
        const file = try loadBudgetFromJson(allocator, json);
        defer file.deinit(allocator);
        return resolveBudgetSpec(file, provider, model);
    }

    const json = std.Io.Dir.cwd().readFileAlloc(zig16_compat.io(), default_budget_json_path, allocator, .limited(1024 * 1024)) catch {
        return resolveBudgetSpec(builtin_budget_file, provider, model);
    };
    defer allocator.free(json);
    const file = loadBudgetFromJson(allocator, json) catch {
        return resolveBudgetSpec(builtin_budget_file, provider, model);
    };
    defer file.deinit(allocator);
    return resolveBudgetSpec(file, provider, model);
}

fn truncateToCharLimit(text: []const u8, limit: usize) []const u8 {
    if (text.len <= limit) return text;
    return text[0..limit];
}

pub fn computeDocumentCharLimits(
    allocator: std.mem.Allocator,
    spec: BudgetSpec,
    doc_ids: []const u64,
    doc_lengths: []const usize,
    system_chars: usize,
    fixed_user_chars: usize,
) !PromptBudgetReport {
    std.debug.assert(doc_ids.len == doc_lengths.len);
    const budget_chars = inputBudgetChars(spec);
    const overhead = system_chars + fixed_user_chars;
    const remaining = if (budget_chars > overhead) budget_chars - overhead else 0;

    var per_doc_limits = try allocator.alloc(usize, doc_lengths.len);
    errdefer allocator.free(per_doc_limits);
    var truncated_doc_ids = std.ArrayList(u64).empty;
    errdefer truncated_doc_ids.deinit(allocator);

    var total_full: usize = 0;
    for (doc_lengths) |len| total_full += len;

    if (total_full <= remaining) {
        for (doc_lengths, 0..) |len, i| {
            per_doc_limits[i] = len;
        }
        return .{
            .provider = "",
            .model = "",
            .input_budget_chars = budget_chars,
            .system_chars = system_chars,
            .fixed_user_chars = fixed_user_chars,
            .per_doc_limits = per_doc_limits,
            .truncated_doc_ids = try truncated_doc_ids.toOwnedSlice(allocator),
            .needs_batch = false,
            .batch_doc_ids = &.{},
        };
    }

    // Proportional allocation when truncation is required.
    var allocated: usize = 0;
    for (doc_lengths, 0..) |len, i| {
        if (total_full == 0) {
            per_doc_limits[i] = 0;
            continue;
        }
        const share = (len * remaining) / total_full;
        const limit = @min(len, share);
        per_doc_limits[i] = limit;
        allocated += limit;
        if (limit < len) try truncated_doc_ids.append(allocator, doc_ids[i]);
    }

    // Distribute leftover chars to docs below full length.
    var leftover = if (remaining > allocated) remaining - allocated else 0;
    while (leftover > 0) {
        var progressed = false;
        for (doc_lengths, 0..) |len, i| {
            if (per_doc_limits[i] >= len) continue;
            per_doc_limits[i] += 1;
            leftover -= 1;
            progressed = true;
            if (leftover == 0) break;
        }
        if (!progressed) break;
    }

    const needs_batch = truncated_doc_ids.items.len > 0;
    return .{
        .provider = "",
        .model = "",
        .input_budget_chars = budget_chars,
        .system_chars = system_chars,
        .fixed_user_chars = fixed_user_chars,
        .per_doc_limits = per_doc_limits,
        .truncated_doc_ids = try truncated_doc_ids.toOwnedSlice(allocator),
        .needs_batch = needs_batch,
        .batch_doc_ids = &.{},
    };
}

pub fn planBudgetBatches(
    allocator: std.mem.Allocator,
    spec: BudgetSpec,
    doc_ids: []const u64,
    doc_lengths: []const usize,
    system_chars: usize,
    fixed_user_chars: usize,
) ![][]u64 {
    std.debug.assert(doc_ids.len == doc_lengths.len);
    if (doc_ids.len == 0) {
        return try allocator.alloc([]u64, 0);
    }

    var batches = std.ArrayList([]u64).empty;
    errdefer {
        for (batches.items) |batch| allocator.free(batch);
        batches.deinit(allocator);
    }

    // Greedy packing on a running character sum. A batch needs no truncation
    // exactly when its total length fits the remaining budget (see
    // computeDocumentCharLimits), so this is equivalent to probing each
    // prefix with computeDocumentCharLimits while staying O(n).
    const budget_chars = inputBudgetChars(spec);
    const overhead = system_chars + fixed_user_chars;
    const remaining = if (budget_chars > overhead) budget_chars - overhead else 0;

    var start: usize = 0;
    while (start < doc_ids.len) {
        // A batch always contains at least one document, even one that must
        // be truncated on its own.
        var end = start + 1;
        var running = doc_lengths[start];
        while (end < doc_ids.len and running + doc_lengths[end] <= remaining) : (end += 1) {
            running += doc_lengths[end];
        }
        try batches.append(allocator, try allocator.dupe(u64, doc_ids[start..end]));
        start = end;
    }

    return try batches.toOwnedSlice(allocator);
}

pub fn writeContextBudgetReport(allocator: std.mem.Allocator, report: PromptBudgetReport) !void {
    const payload = struct {
        input_budget_chars: usize,
        system_chars: usize,
        fixed_user_chars: usize,
        per_doc_limits: []usize,
        truncated_doc_ids: []u64,
        needs_batch: bool,
    }{
        .input_budget_chars = report.input_budget_chars,
        .system_chars = report.system_chars,
        .fixed_user_chars = report.fixed_user_chars,
        .per_doc_limits = report.per_doc_limits,
        .truncated_doc_ids = report.truncated_doc_ids,
        .needs_batch = report.needs_batch,
    };
    const json = try std.json.Stringify.valueAlloc(allocator, payload, .{});
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("CONTEXT_BUDGET_JSON={s}\n", .{json});
    try stderr.flush();
}

pub fn writeContextBudgetBatchReport(allocator: std.mem.Allocator, batches: []const []const u64) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, .{ .batches = batches }, .{});
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("CONTEXT_BUDGET_BATCH_JSON={s}\n", .{json});
    try stderr.flush();
}

pub fn limitedContent(text: []const u8, limit: usize) []const u8 {
    return truncateToCharLimit(text, limit);
}

test "resolveBudgetSpec exact and prefix wildcard" {
    const json =
        \\{"default":{"context_tokens":1000,"output_tokens":100,"prompt_reserve_tokens":50,"chars_per_token":4},"models":[{"provider":"openai","model":"gpt-5-mini","context_tokens":400000,"output_tokens":128000,"prompt_reserve_tokens":32000,"chars_per_token":4},{"provider":"openai","model":"gpt-5*","match":"prefix","context_tokens":200000,"output_tokens":64000,"prompt_reserve_tokens":16000,"chars_per_token":4}]}
    ;
    const file = try loadBudgetFromJson(std.testing.allocator, json);
    defer file.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 400000), resolveBudgetSpec(file, "openai", "gpt-5-mini").context_tokens);
    try std.testing.expectEqual(@as(u64, 200000), resolveBudgetSpec(file, "openai", "gpt-5-nano").context_tokens);
    try std.testing.expectEqual(@as(u64, 1000), resolveBudgetSpec(file, "anthropic", "claude").context_tokens);
}

test "loadBudgetFromJson copies strings out of the parser arena" {
    // "open\u0061i" decodes to "openai" inside the parser arena (escaped
    // strings never alias the input), covering the dangling-slice case;
    // freeing the input before resolving covers the aliasing case.
    const json =
        \\{"default":{"context_tokens":1000,"output_tokens":100,"prompt_reserve_tokens":50,"chars_per_token":4},"models":[{"provider":"open\u0061i","model":"gpt-5-mini","context_tokens":400000,"output_tokens":128000,"prompt_reserve_tokens":32000,"chars_per_token":4}]}
    ;
    const json_copy = try std.testing.allocator.dupe(u8, json);
    const file = try loadBudgetFromJson(std.testing.allocator, json_copy);
    defer file.deinit(std.testing.allocator);
    std.testing.allocator.free(json_copy);

    try std.testing.expectEqualStrings("openai", file.models[0].provider.?);
    try std.testing.expectEqualStrings("gpt-5-mini", file.models[0].model.?);
    try std.testing.expectEqual(@as(u64, 400000), resolveBudgetSpec(file, "openai", "gpt-5-mini").context_tokens);
}

test "planBudgetBatches groups documents by remaining character budget" {
    // remaining chars = (2000 - 400 - 400) * 4 - (1000 + 500) = 3300
    const spec: BudgetSpec = .{
        .context_tokens = 2000,
        .output_tokens = 400,
        .prompt_reserve_tokens = 400,
        .chars_per_token = 4,
    };
    const doc_ids = [_]u64{ 1, 2, 3, 4 };
    const doc_lengths = [_]usize{ 2000, 1000, 9000, 3000 };
    const batches = try planBudgetBatches(std.testing.allocator, spec, &doc_ids, &doc_lengths, 1000, 500);
    defer {
        for (batches) |batch| std.testing.allocator.free(batch);
        std.testing.allocator.free(batches);
    }

    // [1,2] fit together; 3 exceeds the budget alone but still forms a batch;
    // 4 starts a new batch.
    try std.testing.expectEqual(@as(usize, 3), batches.len);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, batches[0]);
    try std.testing.expectEqualSlices(u64, &.{3}, batches[1]);
    try std.testing.expectEqualSlices(u64, &.{4}, batches[2]);
}

test "computeDocumentCharLimits leaves small corpus intact" {
    const doc_ids = [_]u64{ 1, 2, 3 };
    const doc_lengths = [_]usize{ 1000, 1200, 800 };
    const spec: BudgetSpec = .{
        .context_tokens = 400000,
        .output_tokens = 128000,
        .prompt_reserve_tokens = 32000,
        .chars_per_token = 4,
    };
    var report = try computeDocumentCharLimits(std.testing.allocator, spec, &doc_ids, &doc_lengths, 5000, 2000);
    defer report.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), report.truncated_doc_ids.len);
    try std.testing.expect(!report.needs_batch);
    try std.testing.expectEqual(@as(usize, 1000), report.per_doc_limits[0]);
}

test "computeDocumentCharLimits truncates body only under tight budget" {
    const doc_ids = [_]u64{ 1, 2 };
    const doc_lengths = [_]usize{ 5000, 5000 };
    const spec: BudgetSpec = .{
        .context_tokens = 2000,
        .output_tokens = 400,
        .prompt_reserve_tokens = 400,
        .chars_per_token = 4,
    };
    var report = try computeDocumentCharLimits(std.testing.allocator, spec, &doc_ids, &doc_lengths, 1000, 500);
    defer report.deinit(std.testing.allocator);
    try std.testing.expect(report.truncated_doc_ids.len > 0);
    try std.testing.expect(report.per_doc_limits[0] < 5000);
}

test "loadBudget reads bundled fixture" {
    const spec = try loadBudget(std.testing.allocator, "openai", "gpt-5-mini", null);
    try std.testing.expectEqual(@as(u64, 400000), spec.context_tokens);
}
