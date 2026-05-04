const std = @import("std");
const builtin = @import("builtin");

fn requireZig(comptime exact: std.SemanticVersion) void {
    if (builtin.zig_version.major != exact.major or
        builtin.zig_version.minor != exact.minor or
        builtin.zig_version.patch != exact.patch)
    {
        @compileError(std.fmt.comptimePrint(
            "This project requires Zig {f}, but found Zig {f}.",
            .{ exact, builtin.zig_version },
        ));
    }
}

pub fn build(b: *std.Build) void {
    requireZig(.{ .major = 0, .minor = 16, .patch = 0 });

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    configureCroaring(root_mod, .{});

    const ztoon_mod = b.createModule(.{
        .root_source_file = b.path("deps/ztoon/src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    root_mod.addImport("ztoon", ztoon_mod);

    const standalone_http_lib_mod = b.createModule(.{
        .root_source_file = b.path("src/standalone/lib.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    configureCroaring(standalone_http_lib_mod, .{ .disable_neon = true });
    standalone_http_lib_mod.addImport("ztoon", ztoon_mod);

    const benchmark_mod = b.createModule(.{
        .root_source_file = b.path("src/benchmark/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    benchmark_mod.addImport("mindbrain", standalone_http_lib_mod);

    const lib = b.addLibrary(.{
        .name = "pg_mindbrain",
        .root_module = root_mod,
        .linkage = .dynamic,
    });

    var pg_server_include: []const u8 = "/usr/include/postgresql/server";
    var pg_config_code: u8 = 0;
    if (b.runAllowFail(&.{ "pg_config", "--includedir-server" }, &pg_config_code, .ignore)) |stdout| {
        pg_server_include = std.mem.trim(u8, stdout, " \n\r\t");
    } else |_| {
        // Keep the historical fallback paths when pg_config is unavailable.
    }

    root_mod.addSystemIncludePath(.{ .cwd_relative = pg_server_include });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql/16/server" });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql/17/server" });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/postgresql@17/include/server" });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "/usr/local/opt/postgresql@17/include/server" });
    root_mod.addSystemIncludePath(.{ .cwd_relative = "../../.pg_install/include/server" });

    root_mod.addCSourceFile(.{
        .file = b.path("src/common/helper.c"),
        .flags = &.{},
    });

    lib.linker_allow_shlib_undefined = true;

    b.installArtifact(lib);

    // Test root must live in the same module as the source files we want
    // discovered, otherwise their `test ""` blocks are invisible to the test
    // runner. Mirror the dependencies that `standalone_http_lib_mod` declares
    // so each transitively imported file (croaring, ztoon, …) still resolves.
    const standalone_test_mod = b.createModule(.{
        .root_source_file = b.path("src/standalone/tests.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    configureCroaring(standalone_test_mod, .{ .disable_neon = true });
    standalone_test_mod.addImport("ztoon", ztoon_mod);

    const standalone_tests = b.addTest(.{
        .root_module = standalone_test_mod,
    });
    standalone_test_mod.linkSystemLibrary("sqlite3", .{});

    const bm25_search_test_mod = b.createModule(.{
        .root_source_file = b.path("src/mb_facets/bm25/search_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    const bm25_tests = b.addTest(.{
        .root_module = bm25_search_test_mod,
    });

    const run_standalone_tests = b.addRunArtifact(standalone_tests);
    const run_bm25_tests = b.addRunArtifact(bm25_tests);

    // Zig's default build summary only prints failed steps, so successful test runs are silent.
    // Print a short confirmation after both suites pass (skipped automatically if a test fails).
    const print_tests_ok = b.addSystemCommand(&.{
        "sh", "-c",
        \\printf '\n%s\n%s\n\n' \
        \\  'All unit tests passed (standalone + BM25).' \
        \\  'For per-step detail and timing:  zig build test --summary all'
    });
    print_tests_ok.step.dependOn(&run_standalone_tests.step);
    print_tests_ok.step.dependOn(&run_bm25_tests.step);

    const test_standalone_step = b.step("test-standalone", "Run standalone engine tests");
    test_standalone_step.dependOn(&print_tests_ok.step);

    const test_step = b.step("test", "Run all unit tests (standalone engine + BM25)");
    test_step.dependOn(test_standalone_step);

    const standalone_bench_mod = b.createModule(.{
        .root_source_file = b.path("src/standalone/bench.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    configureCroaring(standalone_bench_mod, .{ .disable_neon = true });
    standalone_bench_mod.addImport("ztoon", ztoon_mod);

    const standalone_bench = b.addExecutable(.{
        .name = "standalone-bench",
        .root_module = standalone_bench_mod,
    });
    standalone_bench_mod.linkSystemLibrary("sqlite3", .{});

    const run_standalone_bench = b.addRunArtifact(standalone_bench);
    const bench_standalone_step = b.step("bench-standalone", "Run standalone engine benchmarks");
    bench_standalone_step.dependOn(&run_standalone_bench.step);

    const standalone_tool_mod = b.createModule(.{
        .root_source_file = b.path("src/standalone/tool.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    standalone_tool_mod.addImport("mindbrain", standalone_http_lib_mod);
    standalone_tool_mod.addImport("benchmark", benchmark_mod);

    const standalone_tool = b.addExecutable(.{
        .name = "mindbrain-standalone-tool",
        .root_module = standalone_tool_mod,
    });
    standalone_tool_mod.linkSystemLibrary("sqlite3", .{});

    const install_standalone_tool = b.addInstallArtifact(standalone_tool, .{});
    const standalone_tool_step = b.step("standalone-tool", "Build standalone helper tool");
    standalone_tool_step.dependOn(&install_standalone_tool.step);

    const benchmark_tool_mod = b.createModule(.{
        .root_source_file = b.path("src/benchmark/tool.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    benchmark_tool_mod.addImport("mindbrain", standalone_http_lib_mod);
    benchmark_tool_mod.addImport("benchmark", benchmark_mod);

    const benchmark_tool = b.addExecutable(.{
        .name = "mindbrain-benchmark-tool",
        .root_module = benchmark_tool_mod,
    });
    benchmark_tool_mod.linkSystemLibrary("sqlite3", .{});

    const install_benchmark_tool = b.addInstallArtifact(benchmark_tool, .{});
    const benchmark_tool_step = b.step("benchmark-tool", "Build benchmark helper tool");
    benchmark_tool_step.dependOn(&install_benchmark_tool.step);

    const standalone_http_mod = b.createModule(.{
        .root_source_file = b.path("src/standalone/http_server.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    standalone_http_mod.addImport("mindbrain", standalone_http_lib_mod);

    const standalone_http = b.addExecutable(.{
        .name = "mindbrain-http",
        .root_module = standalone_http_mod,
    });
    standalone_http_mod.linkSystemLibrary("sqlite3", .{});

    const install_standalone_http = b.addInstallArtifact(standalone_http, .{});
    const standalone_http_step = b.step("standalone-http", "Build standalone HTTP server (dashboard API)");
    standalone_http_step.dependOn(&install_standalone_http.step);

    var extension_dir: []const u8 = "/usr/share/postgresql/16/extension";
    pg_config_code = 0;
    if (b.runAllowFail(&.{ "pg_config", "--sharedir" }, &pg_config_code, .ignore)) |stdout| {
        const sharedir = std.mem.trim(u8, stdout, " \n\r\t");
        extension_dir = std.fmt.allocPrint(b.allocator, "{s}/extension", .{sharedir}) catch extension_dir;
    } else |_| {
        // Keep the historical fallback path when pg_config is unavailable.
    }

    _ = b.addInstallFileWithDir(
        b.path("pg_mindbrain.control"),
        .{ .custom = extension_dir },
        "pg_mindbrain.control",
    );

    _ = b.addInstallFileWithDir(
        b.path("sql/sqlite_mindbrain--1.0.0.sql"),
        .{ .custom = extension_dir },
        "sqlite_mindbrain--1.0.0.sql",
    );
}

const CroaringOptions = struct {
    disable_neon: bool = false,
};

fn configureCroaring(module: *std.Build.Module, options: CroaringOptions) void {
    module.addIncludePath(.{ .cwd_relative = "deps/pg_roaringbitmap" });
    if (options.disable_neon) {
        // Keep the standalone Zig header import and the compiled CRoaring object
        // on the same code path for ARM targets.
        module.addCMacro("DISABLENEON", "1");
    }
    module.addCSourceFile(.{
        .file = .{ .cwd_relative = "deps/pg_roaringbitmap/roaring.c" },
        .flags = if (options.disable_neon)
            &.{ "-DCROARING_COMPILER_SUPPORTS_AVX512=0", "-DDISABLENEON=1" }
        else
            &.{"-DCROARING_COMPILER_SUPPORTS_AVX512=0"},
    });
}
