const std = @import("std");

pub const Delimiter = enum {
    comma,
    pipe,
    tab,

    pub fn separator(self: Delimiter) []const u8 {
        return switch (self) {
            .comma => ",",
            .pipe => "|",
            .tab => "\t",
        };
    }

    pub fn headerSuffix(self: Delimiter) []const u8 {
        return switch (self) {
            .comma => "",
            .pipe => "|",
            .tab => "\t",
        };
    }
};

pub const TextWriter = struct {
    buffer: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    indent_width: usize,
    depth: usize = 0,
    needs_indent: bool = true,

    pub fn init(allocator: std.mem.Allocator, indent_width: usize) TextWriter {
        return .{
            .buffer = .empty,
            .allocator = allocator,
            .indent_width = indent_width,
        };
    }

    pub fn deinit(self: *TextWriter) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn finish(self: *TextWriter) ![]u8 {
        return self.buffer.toOwnedSlice(self.allocator);
    }

    pub fn pushIndent(self: *TextWriter) void {
        self.depth += 1;
    }

    pub fn popIndent(self: *TextWriter) void {
        std.debug.assert(self.depth > 0);
        self.depth -= 1;
    }

    pub fn writeAll(self: *TextWriter, bytes: []const u8) !void {
        try self.writeIndentIfNeeded();
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    pub fn writeByte(self: *TextWriter, byte: u8) !void {
        try self.writeIndentIfNeeded();
        try self.buffer.append(self.allocator, byte);
    }

    pub fn newline(self: *TextWriter) !void {
        try self.buffer.append(self.allocator, '\n');
        self.needs_indent = true;
    }

    fn writeIndentIfNeeded(self: *TextWriter) !void {
        if (!self.needs_indent) return;
        self.needs_indent = false;
        try self.buffer.appendNTimes(self.allocator, ' ', self.depth * self.indent_width);
    }
};
