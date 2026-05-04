const std = @import("std");
const builtin = @import("builtin");

var configured_io: std.Io = undefined;
var configured = false;

pub const Timer = struct {
    start_time: std.Io.Timestamp,

    pub fn start() !Timer {
        return .{ .start_time = std.Io.Timestamp.now(io(), .awake) };
    }

    pub fn read(self: *Timer) u64 {
        const elapsed = self.start_time.durationTo(std.Io.Timestamp.now(io(), .awake)).toNanoseconds();
        return @intCast(elapsed);
    }
};

pub fn setIo(process_io: std.Io) void {
    configured_io = process_io;
    configured = true;
}

pub fn io() std.Io {
    if (configured) return configured_io;
    if (builtin.is_test) return std.testing.io;
    @panic("std.Io has not been configured for this executable");
}
