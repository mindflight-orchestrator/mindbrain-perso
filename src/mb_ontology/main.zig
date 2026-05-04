const std = @import("std");
const utils = @import("../mb_pragma/utils.zig");
const ztoon = @import("ztoon");

const c = utils.c;
const PgFinfoRecord = utils.PgFinfoRecord;
const default_options = ztoon.EncodeOptions{
    .delimiter = .tab,
    .indent_width = 2,
};

export fn pg_finfo_json_text_to_toon_native() callconv(.c) *const PgFinfoRecord {
    const info = struct {
        const val = PgFinfoRecord{ .api_version = 1 };
    };
    return &info.val;
}

// On parse / ztoon-convert / encode failure, return the input text (passthrough) so non-null input never becomes SQL NULL.
export fn json_text_to_toon_native(fcinfo: c.FunctionCallInfo) callconv(.c) c.Datum {
    if (utils.is_arg_null(fcinfo, 0)) {
        utils.set_return_null(fcinfo);
        return 0;
    }

    const allocator = utils.PgAllocator.allocator();
    const json_text = utils.textDatumToSlice(utils.get_arg_datum(fcinfo, 0));

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, json_text, .{}) catch {
        return returnJsonPassthrough(fcinfo, json_text);
    };
    defer parsed.deinit();

    const root = ztoon.Value.fromJsonValue(allocator, parsed.value) catch {
        return returnJsonPassthrough(fcinfo, json_text);
    };
    defer deinitOwnedValue(allocator, root);

    const toon = ztoon.encodeAlloc(allocator, root, default_options) catch {
        return returnJsonPassthrough(fcinfo, json_text);
    };
    defer allocator.free(toon);

    return utils.textDatumFromSlice(toon);
}

fn returnJsonPassthrough(fcinfo: c.FunctionCallInfo, json_text: []const u8) c.Datum {
    utils.set_return_not_null(fcinfo);
    return utils.textDatumFromSlice(json_text);
}

fn deinitOwnedValue(allocator: std.mem.Allocator, value: ztoon.Value) void {
    switch (value) {
        .array => |items| {
            for (items) |item| deinitOwnedValue(allocator, item);
            allocator.free(items);
        },
        .object => |fields| {
            for (fields) |field| deinitOwnedValue(allocator, field.value);
            allocator.free(fields);
        },
        else => {},
    }
}
