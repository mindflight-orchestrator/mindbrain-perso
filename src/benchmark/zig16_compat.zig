const std = @import("std");

pub const Timer = struct {
    start_time: std.Io.Timestamp,

    pub fn start() !Timer {
        return .{ .start_time = std.Io.Timestamp.now(benchmark_io, .awake) };
    }

    pub fn read(self: *Timer) u64 {
        const elapsed = self.start_time.durationTo(std.Io.Timestamp.now(benchmark_io, .awake)).toNanoseconds();
        return @intCast(elapsed);
    }
};

var benchmark_io: std.Io = undefined;

pub fn setIo(process_io: std.Io) void {
    benchmark_io = process_io;
}

pub fn io() std.Io {
    return benchmark_io;
}
