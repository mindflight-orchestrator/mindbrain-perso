const std = @import("std");

pub const default_listen_addr = "127.0.0.1:8091";
pub const default_max_body_bytes: usize = 1024 * 1024;
pub const default_max_connections: u32 = 128;
pub const default_sqlite_busy_timeout_ms: u32 = 1_000;

pub const StartupOptions = struct {
    addr_text: []const u8,
    db_path: []const u8,
    static_dir: []const u8,
    init_only: bool,
    max_body_bytes: usize,
    max_connections: u32,
    sqlite_busy_timeout_ms: u32,
};

pub fn resolveStartupOptions(
    args: []const []const u8,
    env_addr: ?[]const u8,
    env_db: ?[]const u8,
    env_static_dir: ?[]const u8,
    env_max_body: ?[]const u8,
    env_max_conns: ?[]const u8,
    env_sqlite_busy_timeout_ms: ?[]const u8,
    printUsageFn: *const fn () anyerror!void,
) !StartupOptions {
    var options = StartupOptions{
        .addr_text = env_addr orelse default_listen_addr,
        .db_path = env_db orelse "data/mindbrain.sqlite",
        .static_dir = env_static_dir orelse "dashboard/dist",
        .init_only = false,
        .max_body_bytes = if (env_max_body) |value| try parsePositiveUsize(value) else default_max_body_bytes,
        .max_connections = if (env_max_conns) |value| try parsePositiveU32(value) else default_max_connections,
        .sqlite_busy_timeout_ms = if (env_sqlite_busy_timeout_ms) |value| try parsePositiveU32(value) else default_sqlite_busy_timeout_ms,
    };

    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--addr")) {
            index += 1;
            if (index >= args.len) return error.InvalidArguments;
            options.addr_text = args[index];
        } else if (std.mem.eql(u8, arg, "--db") or std.mem.eql(u8, arg, "--mindbrain-db")) {
            index += 1;
            if (index >= args.len) return error.InvalidArguments;
            options.db_path = args[index];
        } else if (std.mem.startsWith(u8, arg, "--addr=")) {
            options.addr_text = arg["--addr=".len..];
        } else if (std.mem.startsWith(u8, arg, "--db=")) {
            options.db_path = arg["--db=".len..];
        } else if (std.mem.startsWith(u8, arg, "--mindbrain-db=")) {
            options.db_path = arg["--mindbrain-db=".len..];
        } else if (std.mem.eql(u8, arg, "--static-dir")) {
            index += 1;
            if (index >= args.len) return error.InvalidArguments;
            options.static_dir = args[index];
        } else if (std.mem.startsWith(u8, arg, "--static-dir=")) {
            options.static_dir = arg["--static-dir=".len..];
        } else if (std.mem.eql(u8, arg, "--init-only")) {
            options.init_only = true;
        } else if (std.mem.eql(u8, arg, "--sqlite-busy-timeout-ms")) {
            index += 1;
            if (index >= args.len) return error.InvalidArguments;
            options.sqlite_busy_timeout_ms = try parsePositiveU32(args[index]);
        } else if (std.mem.startsWith(u8, arg, "--sqlite-busy-timeout-ms=")) {
            options.sqlite_busy_timeout_ms = try parsePositiveU32(arg["--sqlite-busy-timeout-ms=".len..]);
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try printUsageFn();
            return error.InvalidArguments;
        } else {
            return error.InvalidArguments;
        }
    }

    return options;
}

pub fn parseListenAddress(text: []const u8) !std.Io.net.IpAddress {
    if (text.len > 0 and text[0] == ':') {
        const port = try parsePort(text[1..]);
        return try std.Io.net.IpAddress.parse("0.0.0.0", port);
    }
    return try std.Io.net.IpAddress.parseLiteral(text);
}

pub fn isLoopbackListenText(text: []const u8) bool {
    const host = extractListenHost(text) orelse return false;
    return std.mem.eql(u8, host, "127.0.0.1") or
        std.mem.eql(u8, host, "::1") or
        std.mem.eql(u8, host, "0:0:0:0:0:0:0:1");
}

fn parsePort(text: []const u8) !u16 {
    return std.fmt.parseInt(u16, text, 10);
}

fn parsePositiveUsize(text: []const u8) !usize {
    const value = try std.fmt.parseInt(usize, text, 10);
    if (value == 0) return error.InvalidArguments;
    return value;
}

fn parsePositiveU32(text: []const u8) !u32 {
    const value = try std.fmt.parseInt(u32, text, 10);
    if (value == 0) return error.InvalidArguments;
    return value;
}

fn extractListenHost(text: []const u8) ?[]const u8 {
    if (text.len == 0 or text[0] == ':') return null;
    if (text[0] == '[') {
        const close_idx = std.mem.indexOfScalar(u8, text, ']') orelse return null;
        return text[1..close_idx];
    }
    const colon_idx = std.mem.lastIndexOfScalar(u8, text, ':') orelse return null;
    return text[0..colon_idx];
}

fn noopUsage() !void {}

test "startup options default to loopback and env limits" {
    const options = try resolveStartupOptions(
        &.{"mindbrain-http"},
        null,
        null,
        null,
        "4096",
        "7",
        "1234",
        noopUsage,
    );

    try std.testing.expectEqualStrings("127.0.0.1:8091", options.addr_text);
    try std.testing.expectEqualStrings("data/mindbrain.sqlite", options.db_path);
    try std.testing.expectEqualStrings("dashboard/dist", options.static_dir);
    try std.testing.expectEqual(@as(usize, 4096), options.max_body_bytes);
    try std.testing.expectEqual(@as(u32, 7), options.max_connections);
    try std.testing.expectEqual(@as(u32, 1234), options.sqlite_busy_timeout_ms);
}

test "startup options prefer env listen addr when cli is absent" {
    const options = try resolveStartupOptions(
        &.{"mindbrain-http"},
        "0.0.0.0:9000",
        null,
        null,
        null,
        null,
        null,
        noopUsage,
    );

    try std.testing.expectEqualStrings("0.0.0.0:9000", options.addr_text);
}

test "startup options let cli override env listen addr" {
    const options = try resolveStartupOptions(
        &.{ "mindbrain-http", "--addr", "127.0.0.1:7001" },
        "0.0.0.0:9000",
        null,
        null,
        null,
        null,
        null,
        noopUsage,
    );

    try std.testing.expectEqualStrings("127.0.0.1:7001", options.addr_text);
}

test "parseListenAddress supports bracketed IPv6" {
    _ = try parseListenAddress("[::1]:8091");
}

test "loopback listen text detection matches secure default" {
    try std.testing.expect(isLoopbackListenText("127.0.0.1:8091"));
    try std.testing.expect(isLoopbackListenText("[::1]:8091"));
    try std.testing.expect(!isLoopbackListenText(":8091"));
    try std.testing.expect(!isLoopbackListenText("0.0.0.0:8091"));
}
