const std = @import("std");
const mindbrain = @import("mindbrain");

pub fn main(init: std.process.Init) !void {
    mindbrain.zig16_compat.setIo(init.io);
    var app = try mindbrain.http_app.MindbrainHttpApp.init(
        init.gpa,
        init.io,
        try init.minimal.args.toSlice(init.arena.allocator()),
        init.environ_map,
    );
    defer app.deinit();

    if (app.init_only) return;

    try app.serve();
}
