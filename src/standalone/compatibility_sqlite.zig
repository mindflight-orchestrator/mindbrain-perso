pub const CompatibilityFlags = struct {
    native_tokenization: bool,
    bm25_sync_hooks: bool,
    ddl_triggers_supported: bool,
    parallel_indexing: bool,
    unlogged_tables: bool,
};

const supported_upstream_versions = [_][]const u8{ "0.3.9", "0.4.1", "0.4.3" };

pub fn supportedUpstreamVersions() []const []const u8 {
    return supported_upstream_versions[0..];
}

pub fn compatibilityFlags() CompatibilityFlags {
    return .{
        .native_tokenization = true,
        .bm25_sync_hooks = true,
        .ddl_triggers_supported = false,
        .parallel_indexing = false,
        .unlogged_tables = false,
    };
}
