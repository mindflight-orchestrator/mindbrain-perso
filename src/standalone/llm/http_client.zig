const std = @import("std");

pub const JsonResponse = struct {
    body: []u8,

    pub fn deinit(self: JsonResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

/// Retry/backoff policy for transient failures (connection errors and
/// 408/429/5xx responses). Non-transient failures never retry.
pub const RetryPolicy = struct {
    /// Total attempts, including the first one.
    max_attempts: u8 = 3,
    /// Backoff before the first retry; doubles per retry up to `max_backoff_ms`.
    initial_backoff_ms: u64 = 250,
    max_backoff_ms: u64 = 4_000,
    /// Total wall-clock budget across attempts and backoffs. Once exceeded no
    /// further retries start. Note: an in-flight request is not interrupted
    /// mid-read; the deadline bounds retrying, not a single hung socket.
    total_deadline_ms: u64 = 120_000,
};

pub const RequestOptions = struct {
    /// Maximum bytes of response body buffered in memory. Exceeding it
    /// returns `error.ResponseTooLarge`.
    max_response_bytes: usize = 4 * 1024 * 1024,
    retry: RetryPolicy = .{},
};

threadlocal var last_http_status: ?u16 = null;
threadlocal var last_http_body: ?[]u8 = null;
threadlocal var last_http_allocator: ?std.mem.Allocator = null;

pub fn lastHttpStatus() ?u16 {
    return last_http_status;
}

pub fn clearLastHttpFailure() void {
    if (last_http_body) |body| {
        if (last_http_allocator) |allocator| allocator.free(body);
    }
    last_http_status = null;
    last_http_body = null;
    last_http_allocator = null;
}

pub fn lastHttpFailureJson(allocator: std.mem.Allocator) !?[]u8 {
    const status = last_http_status orelse return null;
    const body = last_http_body orelse "";
    return try std.json.Stringify.valueAlloc(allocator, .{
        .ok = false,
        .@"error" = "HttpRequestFailed",
        .status = status,
        .body = body,
    }, .{});
}

fn rememberHttpFailure(allocator: std.mem.Allocator, status: std.http.Status, body: []const u8) void {
    clearLastHttpFailure();
    // Copy the body before publishing any state so a failed dupe cannot leave
    // a status pointing at a stale/absent body. On OOM keep the status only.
    const copy = allocator.dupe(u8, body) catch null;
    last_http_status = @intFromEnum(status);
    if (copy) |owned| {
        last_http_body = owned;
        last_http_allocator = allocator;
    }
}

pub fn postJson(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    api_key: ?[]const u8,
    payload: []const u8,
    options: RequestOptions,
) !JsonResponse {
    var headers_buf: [4]std.http.Header = undefined;
    var header_count: usize = 0;
    headers_buf[header_count] = .{ .name = "content-type", .value = "application/json" };
    header_count += 1;

    var auth_header: ?[]u8 = null;
    defer if (auth_header) |h| allocator.free(h);
    if (api_key) |key| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{key});
        headers_buf[header_count] = .{ .name = "authorization", .value = auth_header.? };
        header_count += 1;
    }

    return post(allocator, io, url, payload, headers_buf[0..header_count], options);
}

pub fn postWithHeaders(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    payload: []const u8,
    headers: []const Header,
    options: RequestOptions,
) !JsonResponse {
    var headers_buf: [16]std.http.Header = undefined;
    if (headers.len > headers_buf.len) return error.TooManyHeaders;
    for (headers, 0..) |header, i| {
        headers_buf[i] = .{ .name = header.name, .value = header.value };
    }
    return post(allocator, io, url, payload, headers_buf[0..headers.len], options);
}

const AttemptResult = union(enum) {
    ok: []u8,
    http_failure: struct {
        status: std.http.Status,
        body: []u8,
    },
};

fn post(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    payload: []const u8,
    headers: []const std.http.Header,
    options: RequestOptions,
) !JsonResponse {
    var client: std.http.Client = .{
        .allocator = allocator,
        .io = io,
    };
    defer client.deinit();

    clearLastHttpFailure();

    const start = std.Io.Timestamp.now(io, .awake);
    var attempt: u8 = 1;
    while (true) : (attempt += 1) {
        const result = attemptPost(allocator, &client, url, payload, headers, options.max_response_bytes) catch |err| {
            if (isRetryableTransportError(err) and
                shouldRetryAfterBackoff(io, start, attempt, options.retry)) continue;
            return err;
        };
        switch (result) {
            .ok => |body| return .{ .body = body },
            .http_failure => |failure| {
                if (isRetryableStatus(failure.status) and
                    shouldRetryAfterBackoff(io, start, attempt, options.retry))
                {
                    allocator.free(failure.body);
                    continue;
                }
                rememberHttpFailure(allocator, failure.status, failure.body);
                var stderr_file_writer = std.Io.File.stderr().writer(io, &.{});
                const stderr = &stderr_file_writer.interface;
                stderr.print("LLM HTTP request failed: status={d} body={s}\n", .{
                    @intFromEnum(failure.status),
                    failure.body,
                }) catch {};
                stderr.flush() catch {};
                allocator.free(failure.body);
                return error.HttpRequestFailed;
            },
        }
    }
}

/// Performs a single POST. The returned body slice is owned by the caller.
/// The response body is capped at `max_response_bytes`
/// (`error.ResponseTooLarge` past that).
fn attemptPost(
    allocator: std.mem.Allocator,
    client: *std.http.Client,
    url: []const u8,
    payload: []const u8,
    headers: []const std.http.Header,
    max_response_bytes: usize,
) !AttemptResult {
    const uri = try std.Uri.parse(url);
    var req = try client.request(.POST, uri, .{
        .extra_headers = headers,
        .keep_alive = true,
        // POST payloads are never re-sent across redirects; match std fetch.
        .redirect_behavior = .unhandled,
    });
    defer req.deinit();

    req.transfer_encoding = .{ .content_length = payload.len };
    var body_writer = try req.sendBodyUnflushed(&.{});
    try body_writer.writer.writeAll(payload);
    try body_writer.end();
    try req.connection.?.flush();

    var response = try req.receiveHead(&.{});
    const status = response.head.status;

    const decompress_buffer: []u8 = switch (response.head.content_encoding) {
        .identity => &.{},
        .zstd => try allocator.alloc(u8, std.compress.zstd.default_window_len),
        .deflate, .gzip => try allocator.alloc(u8, std.compress.flate.max_window_len),
        .compress => return error.UnsupportedCompressionMethod,
    };
    defer if (decompress_buffer.len > 0) allocator.free(decompress_buffer);

    var transfer_buffer: [64]u8 = undefined;
    var decompress: std.http.Decompress = undefined;
    const reader = response.readerDecompressing(&transfer_buffer, &decompress, decompress_buffer);

    const body = reader.allocRemaining(allocator, .limited(max_response_bytes)) catch |err| switch (err) {
        error.StreamTooLong => return error.ResponseTooLarge,
        error.ReadFailed => return response.bodyErr() orelse error.ReadFailed,
        error.OutOfMemory => return error.OutOfMemory,
    };
    errdefer allocator.free(body);

    if (status.class() == .success) return .{ .ok = body };
    return .{ .http_failure = .{ .status = status, .body = body } };
}

fn isRetryableStatus(status: std.http.Status) bool {
    if (status.class() == .server_error) return true;
    return switch (status) {
        .request_timeout, .too_many_requests => true,
        else => false,
    };
}

fn isRetryableTransportError(err: anyerror) bool {
    return switch (err) {
        // Connection-level failures that a fresh attempt can plausibly fix.
        error.ConnectionResetByPeer,
        error.ConnectionRefused,
        error.ConnectionTimedOut,
        error.NetworkUnreachable,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.EndOfStream,
        error.ReadFailed,
        error.WriteFailed,
        => true,
        else => false,
    };
}

/// Returns true after sleeping the backoff for `attempt` when another attempt
/// is allowed by `retry`; returns false when attempts or the total deadline
/// are exhausted (or the sleep was canceled).
fn shouldRetryAfterBackoff(io: std.Io, start: std.Io.Timestamp, attempt: u8, retry: RetryPolicy) bool {
    if (attempt >= retry.max_attempts) return false;
    const elapsed = start.durationTo(std.Io.Timestamp.now(io, .awake)).toMilliseconds();
    if (elapsed < 0) return false;
    const shift: u6 = @intCast(@min(attempt - 1, 62));
    const backoff_ms = @min(
        retry.initial_backoff_ms *| (@as(u64, 1) << shift),
        retry.max_backoff_ms,
    );
    if (@as(u64, @intCast(elapsed)) +| backoff_ms >= retry.total_deadline_ms) return false;
    io.sleep(.fromMilliseconds(@intCast(backoff_ms)), .awake) catch return false;
    return true;
}

pub fn trimRight(text: []const u8, byte: u8) []const u8 {
    var end = text.len;
    while (end > 0 and text[end - 1] == byte) : (end -= 1) {}
    return text[0..end];
}

test "lastHttpFailureJson exposes status and response body" {
    clearLastHttpFailure();
    rememberHttpFailure(std.testing.allocator, .bad_request, "{\"error\":{\"message\":\"bad request\"}}");
    defer clearLastHttpFailure();
    const json = (try lastHttpFailureJson(std.testing.allocator)).?;
    defer std.testing.allocator.free(json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"status\":400") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "bad request") != null);
}

test "retry backoff respects attempt cap and total deadline" {
    const io = std.testing.io;
    const start = std.Io.Timestamp.now(io, .awake);
    const policy: RetryPolicy = .{
        .max_attempts = 2,
        .initial_backoff_ms = 0,
        .max_backoff_ms = 0,
        .total_deadline_ms = 60_000,
    };
    try std.testing.expect(shouldRetryAfterBackoff(io, start, 1, policy));
    try std.testing.expect(!shouldRetryAfterBackoff(io, start, 2, policy));

    const expired: RetryPolicy = .{ .max_attempts = 3, .total_deadline_ms = 0 };
    try std.testing.expect(!shouldRetryAfterBackoff(io, start, 1, expired));
}

test "retryable status classification" {
    try std.testing.expect(isRetryableStatus(.too_many_requests));
    try std.testing.expect(isRetryableStatus(.request_timeout));
    try std.testing.expect(isRetryableStatus(.internal_server_error));
    try std.testing.expect(isRetryableStatus(.bad_gateway));
    try std.testing.expect(!isRetryableStatus(.bad_request));
    try std.testing.expect(!isRetryableStatus(.unauthorized));
    try std.testing.expect(!isRetryableStatus(.not_found));
}
