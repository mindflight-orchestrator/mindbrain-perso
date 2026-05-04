//! Document chunker for the standalone ingest pipeline.
//!
//! The chunker turns a single source document into an ordered list of
//! `Chunk`s ready to be persisted into `chunks_raw` (and, downstream, fed
//! to embedders and BM25 indexing).
//!
//! Two families of strategies live here:
//!   * Deterministic — `fixed_token`, `sentence`, `paragraph`,
//!     `recursive_character`, `structure_aware` — these only need the source
//!     text and a few options. They are implemented in this file.
//!   * Embedding-aware — `semantic`, `late` — these need an out-of-band
//!     embedding callback and are implemented in `chunker_semantic.zig`
//!     (added by the next task in the plan).
//!
//! `Chunk.content` is always a borrowed slice into the input `text`. The
//! caller therefore owns the lifetime of the original text for as long as
//! the returned chunks are in use; only the `[]Chunk` slice itself is
//! allocated on `allocator` and freed via `freeChunks`.

const std = @import("std");
const collections = @import("collections_sqlite.zig");

pub const Strategy = enum {
    fixed_token,
    sentence,
    paragraph,
    recursive_character,
    structure_aware,
    semantic,
    late,

    pub fn label(self: Strategy) []const u8 {
        return switch (self) {
            .fixed_token => "fixed_token",
            .sentence => "sentence",
            .paragraph => "paragraph",
            .recursive_character => "recursive_character",
            .structure_aware => "structure_aware",
            .semantic => "semantic",
            .late => "late",
        };
    }
};

pub const Chunk = struct {
    /// 0-based ordinal in the produced chunk stream. Stable for a given
    /// strategy + options pair, which is what makes the
    /// `<doc_nanoid>#<chunk_index>` external addressing scheme reproducible.
    index: u32,
    /// Borrowed slice into the source `text` passed to `chunk(...)`.
    content: []const u8,
    /// Inclusive byte offset into `text` where this chunk starts.
    offset_start: usize,
    /// Exclusive byte offset into `text` where this chunk ends.
    offset_end: usize,
    /// Whitespace-separated word count under `countTokens`; persisted into
    /// `chunks_raw.token_count` so the LLM context budgeter does not need to
    /// re-tokenize.
    token_count: u64,
    /// Set by hierarchical strategies (e.g. `late`) to the `index` of the
    /// containing parent slice; `null` for flat strategies.
    parent_chunk_index: ?u32 = null,
    /// Static label of the strategy that produced this chunk.
    strategy: []const u8,
};

pub const Options = struct {
    strategy: Strategy = .recursive_character,
    /// Token target for `fixed_token` and the upper bound considered by
    /// `recursive_character` / `structure_aware` when collapsing groups.
    target_tokens: u32 = 256,
    /// Token overlap between successive `fixed_token` chunks.
    overlap_tokens: u32 = 32,
    /// Soft byte ceiling for character-oriented strategies. The recursive
    /// splitter will keep dividing until every chunk fits under
    /// `max_chars`.
    max_chars: usize = 2048,
    /// Optional minimum chunk size (chars). Used to merge a tiny trailing
    /// chunk back into its predecessor.
    min_chars: usize = 32,
};

pub const Error = error{
    InvalidOptions,
} || std.mem.Allocator.Error;

/// Frees the slice returned by `chunk`. Does not touch chunk contents,
/// which are borrowed from the source text.
pub fn freeChunks(allocator: std.mem.Allocator, chunks: []Chunk) void {
    allocator.free(chunks);
}

/// Splits `text` according to `options`. Embedding-aware strategies are
/// rejected here (they live in `chunker_semantic.zig`).
pub fn chunk(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) Error![]Chunk {
    if (options.target_tokens == 0 and options.strategy == .fixed_token) {
        return error.InvalidOptions;
    }
    if (options.overlap_tokens >= options.target_tokens and options.strategy == .fixed_token) {
        return error.InvalidOptions;
    }

    return switch (options.strategy) {
        .fixed_token => chunkFixedToken(allocator, text, options),
        .sentence => chunkSentences(allocator, text, options),
        .paragraph => chunkParagraphs(allocator, text, options),
        .recursive_character => chunkRecursive(allocator, text, options),
        .structure_aware => chunkStructureAware(allocator, text, options),
        .semantic, .late => error.InvalidOptions,
    };
}

// ---- Token utilities ------------------------------------------------------

/// Counts whitespace/punctuation-separated word tokens. This intentionally
/// keeps stop words and short tokens (unlike `tokenization_sqlite`) because
/// chunkers care about LLM context budgets, not BM25 vocabulary.
pub fn countTokens(text: []const u8) u64 {
    var count: u64 = 0;
    var i: usize = 0;
    while (i < text.len) {
        while (i < text.len and !isWordByte(text[i])) : (i += 1) {}
        if (i >= text.len) break;
        const start = i;
        while (i < text.len and isWordByte(text[i])) : (i += 1) {}
        if (i > start) count += 1;
    }
    return count;
}

const TokenSpan = struct {
    start: usize,
    end: usize,
};

fn collectTokenSpans(allocator: std.mem.Allocator, text: []const u8) Error![]TokenSpan {
    var spans = std.ArrayList(TokenSpan).empty;
    errdefer spans.deinit(allocator);
    var i: usize = 0;
    while (i < text.len) {
        while (i < text.len and !isWordByte(text[i])) : (i += 1) {}
        if (i >= text.len) break;
        const start = i;
        while (i < text.len and isWordByte(text[i])) : (i += 1) {}
        try spans.append(allocator, .{ .start = start, .end = i });
    }
    return spans.toOwnedSlice(allocator);
}

fn isWordByte(b: u8) bool {
    return std.ascii.isAlphanumeric(b) or b >= 0x80;
}

// ---- fixed_token ----------------------------------------------------------

fn chunkFixedToken(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) Error![]Chunk {
    if (text.len == 0) return allocator.alloc(Chunk, 0);

    const spans = try collectTokenSpans(allocator, text);
    defer allocator.free(spans);

    if (spans.len == 0) return allocator.alloc(Chunk, 0);

    var chunks = std.ArrayList(Chunk).empty;
    errdefer chunks.deinit(allocator);

    const stride: u32 = options.target_tokens - options.overlap_tokens;
    var token_cursor: usize = 0;
    var index: u32 = 0;
    while (token_cursor < spans.len) {
        const window_end = @min(token_cursor + options.target_tokens, spans.len);
        const start_byte = spans[token_cursor].start;
        const end_byte = spans[window_end - 1].end;
        const slice = text[start_byte..end_byte];
        try chunks.append(allocator, .{
            .index = index,
            .content = slice,
            .offset_start = start_byte,
            .offset_end = end_byte,
            .token_count = @intCast(window_end - token_cursor),
            .strategy = Strategy.fixed_token.label(),
        });
        index += 1;
        if (window_end == spans.len) break;
        token_cursor += stride;
    }

    return chunks.toOwnedSlice(allocator);
}

// ---- paragraph ------------------------------------------------------------

fn chunkParagraphs(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) Error![]Chunk {
    _ = options;
    if (text.len == 0) return allocator.alloc(Chunk, 0);

    var chunks = std.ArrayList(Chunk).empty;
    errdefer chunks.deinit(allocator);

    var index: u32 = 0;
    var cursor: usize = 0;
    while (cursor < text.len) {
        // Skip leading blank-line whitespace.
        while (cursor < text.len and isWhitespaceByte(text[cursor])) : (cursor += 1) {}
        if (cursor >= text.len) break;

        const start = cursor;
        // Walk until we see two consecutive newlines (allowing CR).
        var end = cursor;
        while (end < text.len) {
            if (isParagraphBreak(text, end)) break;
            end += 1;
        }
        // Trim trailing whitespace within the paragraph.
        var paragraph_end = end;
        while (paragraph_end > start and isWhitespaceByte(text[paragraph_end - 1])) : (paragraph_end -= 1) {}
        if (paragraph_end > start) {
            const slice = text[start..paragraph_end];
            try chunks.append(allocator, .{
                .index = index,
                .content = slice,
                .offset_start = start,
                .offset_end = paragraph_end,
                .token_count = countTokens(slice),
                .strategy = Strategy.paragraph.label(),
            });
            index += 1;
        }
        cursor = end;
    }

    return chunks.toOwnedSlice(allocator);
}

fn isParagraphBreak(text: []const u8, i: usize) bool {
    if (i >= text.len or text[i] != '\n') return false;
    var j = i + 1;
    // Allow a single CR between the two LFs.
    if (j < text.len and text[j] == '\r') j += 1;
    return j < text.len and text[j] == '\n';
}

fn isWhitespaceByte(b: u8) bool {
    return b == ' ' or b == '\t' or b == '\n' or b == '\r';
}

// ---- sentence -------------------------------------------------------------

fn chunkSentences(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) Error![]Chunk {
    if (text.len == 0) return allocator.alloc(Chunk, 0);

    const sentence_spans = try collectSentenceSpans(allocator, text);
    defer allocator.free(sentence_spans);

    var chunks = std.ArrayList(Chunk).empty;
    errdefer chunks.deinit(allocator);

    var index: u32 = 0;
    var i: usize = 0;
    while (i < sentence_spans.len) {
        const start_byte = sentence_spans[i].start;
        var end_byte = sentence_spans[i].end;
        var j = i + 1;
        // Group successive sentences while we stay under max_chars.
        while (j < sentence_spans.len and (sentence_spans[j].end - start_byte) <= options.max_chars) : (j += 1) {
            end_byte = sentence_spans[j].end;
        }
        const slice = text[start_byte..end_byte];
        try chunks.append(allocator, .{
            .index = index,
            .content = slice,
            .offset_start = start_byte,
            .offset_end = end_byte,
            .token_count = countTokens(slice),
            .strategy = Strategy.sentence.label(),
        });
        index += 1;
        i = if (j == i) i + 1 else j;
    }

    return chunks.toOwnedSlice(allocator);
}

const SentenceSpan = TokenSpan;

fn collectSentenceSpans(allocator: std.mem.Allocator, text: []const u8) Error![]SentenceSpan {
    var spans = std.ArrayList(SentenceSpan).empty;
    errdefer spans.deinit(allocator);

    var cursor: usize = 0;
    while (cursor < text.len) {
        while (cursor < text.len and isWhitespaceByte(text[cursor])) : (cursor += 1) {}
        if (cursor >= text.len) break;
        const start = cursor;
        var end = cursor;
        while (end < text.len) {
            const ch = text[end];
            end += 1;
            if (ch == '.' or ch == '!' or ch == '?') {
                if (end >= text.len) break;
                if (isSentenceTerminator(text, end)) break;
            } else if (ch == '\n' and end < text.len and text[end] == '\n') {
                break;
            }
        }
        // Trim trailing whitespace.
        var trimmed_end = end;
        while (trimmed_end > start and isWhitespaceByte(text[trimmed_end - 1])) : (trimmed_end -= 1) {}
        if (trimmed_end > start) {
            try spans.append(allocator, .{ .start = start, .end = trimmed_end });
        }
        cursor = end;
    }
    return spans.toOwnedSlice(allocator);
}

fn isSentenceTerminator(text: []const u8, i: usize) bool {
    return text[i] == ' ' or text[i] == '\t' or text[i] == '\n' or text[i] == '\r';
}

// ---- recursive_character --------------------------------------------------

const recursive_separators = [_][]const u8{ "\n\n", "\n", ". ", "? ", "! ", "; ", ", ", " " };

fn chunkRecursive(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) Error![]Chunk {
    if (text.len == 0) return allocator.alloc(Chunk, 0);

    var pieces = std.ArrayList(TokenSpan).empty;
    defer pieces.deinit(allocator);
    try splitRecursive(allocator, text, 0, text.len, options, &recursive_separators, 0, &pieces);

    if (pieces.items.len == 0) return allocator.alloc(Chunk, 0);

    var chunks = try allocator.alloc(Chunk, pieces.items.len);
    var idx: u32 = 0;
    for (pieces.items) |span| {
        const slice = text[span.start..span.end];
        chunks[idx] = .{
            .index = idx,
            .content = slice,
            .offset_start = span.start,
            .offset_end = span.end,
            .token_count = countTokens(slice),
            .strategy = Strategy.recursive_character.label(),
        };
        idx += 1;
    }
    return chunks;
}

fn splitRecursive(
    allocator: std.mem.Allocator,
    text: []const u8,
    start: usize,
    end: usize,
    options: Options,
    separators: []const []const u8,
    depth: usize,
    out: *std.ArrayList(TokenSpan),
) Error!void {
    const slice_len = end - start;
    if (slice_len == 0) return;

    if (slice_len <= options.max_chars or depth >= separators.len) {
        const trimmed = trimSpan(text, start, end);
        if (trimmed.end > trimmed.start) try out.append(allocator, trimmed);
        return;
    }

    const sep = separators[depth];
    var cursor = start;
    var current_start = start;
    var any_split = false;
    while (cursor + sep.len <= end) {
        if (std.mem.startsWith(u8, text[cursor..end], sep)) {
            any_split = true;
            const piece_end = cursor + sep.len;
            try splitRecursive(allocator, text, current_start, piece_end, options, separators, depth + 1, out);
            current_start = piece_end;
            cursor = piece_end;
        } else {
            cursor += 1;
        }
    }
    if (current_start < end) {
        try splitRecursive(allocator, text, current_start, end, options, separators, depth + 1, out);
    }
    if (!any_split) {
        try splitRecursive(allocator, text, start, end, options, separators, depth + 1, out);
    }
}

fn trimSpan(text: []const u8, start: usize, end: usize) TokenSpan {
    var s = start;
    var e = end;
    while (s < e and isWhitespaceByte(text[s])) : (s += 1) {}
    while (e > s and isWhitespaceByte(text[e - 1])) : (e -= 1) {}
    return .{ .start = s, .end = e };
}

// ---- structure_aware -----------------------------------------------------

fn chunkStructureAware(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
) Error![]Chunk {
    if (text.len == 0) return allocator.alloc(Chunk, 0);

    const sections = try collectStructureSections(allocator, text);
    defer allocator.free(sections);

    var chunks = std.ArrayList(Chunk).empty;
    errdefer chunks.deinit(allocator);

    var idx: u32 = 0;
    for (sections) |section| {
        if (section.end - section.start <= options.max_chars) {
            const slice = text[section.start..section.end];
            try chunks.append(allocator, .{
                .index = idx,
                .content = slice,
                .offset_start = section.start,
                .offset_end = section.end,
                .token_count = countTokens(slice),
                .strategy = Strategy.structure_aware.label(),
            });
            idx += 1;
            continue;
        }
        // Section is too big: re-run the recursive splitter on its body and
        // keep its boundaries as a parent index.
        const sub_chunks = try chunkRecursive(allocator, text[section.start..section.end], options);
        defer allocator.free(sub_chunks);
        for (sub_chunks) |sc| {
            try chunks.append(allocator, .{
                .index = idx,
                .content = sc.content,
                .offset_start = section.start + sc.offset_start,
                .offset_end = section.start + sc.offset_end,
                .token_count = sc.token_count,
                .parent_chunk_index = null,
                .strategy = Strategy.structure_aware.label(),
            });
            idx += 1;
        }
    }

    return chunks.toOwnedSlice(allocator);
}

fn collectStructureSections(allocator: std.mem.Allocator, text: []const u8) Error![]TokenSpan {
    var sections = std.ArrayList(TokenSpan).empty;
    errdefer sections.deinit(allocator);

    var cursor: usize = 0;
    var section_start: usize = 0;
    var in_code_fence = false;

    while (cursor < text.len) {
        // Look at the start of each line.
        const line_start = cursor;
        const line_end = lineEnd(text, cursor);
        const line = text[line_start..line_end];

        const fence = startsWithFence(line);
        if (fence) {
            // Flush any open section before / after a fence boundary so that
            // code blocks become standalone sections.
            if (section_start < line_start) {
                try appendTrimmed(allocator, &sections, text, section_start, line_start);
            }
            // Find matching fence end.
            var inner_end = line_end + 1;
            in_code_fence = true;
            while (inner_end < text.len) {
                const inner_line_end = lineEnd(text, inner_end);
                if (startsWithFence(text[inner_end..inner_line_end])) {
                    inner_end = inner_line_end + 1;
                    in_code_fence = false;
                    break;
                }
                inner_end = inner_line_end + 1;
            }
            try appendTrimmed(allocator, &sections, text, line_start, @min(inner_end, text.len));
            section_start = @min(inner_end, text.len);
            cursor = section_start;
            continue;
        }

        if (isMarkdownHeading(line)) {
            if (section_start < line_start) {
                try appendTrimmed(allocator, &sections, text, section_start, line_start);
                section_start = line_start;
            }
        }
        cursor = line_end + 1;
    }

    if (section_start < text.len) {
        try appendTrimmed(allocator, &sections, text, section_start, text.len);
    }

    if (sections.items.len == 0) {
        try sections.append(allocator, .{ .start = 0, .end = text.len });
    }

    return sections.toOwnedSlice(allocator);
}

fn lineEnd(text: []const u8, start: usize) usize {
    var i = start;
    while (i < text.len and text[i] != '\n') : (i += 1) {}
    return i;
}

fn startsWithFence(line: []const u8) bool {
    var trimmed = line;
    while (trimmed.len > 0 and (trimmed[0] == ' ' or trimmed[0] == '\t')) : (trimmed = trimmed[1..]) {}
    return std.mem.startsWith(u8, trimmed, "```") or std.mem.startsWith(u8, trimmed, "~~~");
}

fn isMarkdownHeading(line: []const u8) bool {
    var trimmed = line;
    while (trimmed.len > 0 and (trimmed[0] == ' ' or trimmed[0] == '\t')) : (trimmed = trimmed[1..]) {}
    if (trimmed.len == 0 or trimmed[0] != '#') return false;
    var depth: usize = 0;
    while (depth < trimmed.len and trimmed[depth] == '#') : (depth += 1) {}
    if (depth > 6) return false;
    return depth < trimmed.len and (trimmed[depth] == ' ' or trimmed[depth] == '\t');
}

fn appendTrimmed(
    allocator: std.mem.Allocator,
    list: *std.ArrayList(TokenSpan),
    text: []const u8,
    start: usize,
    end: usize,
) Error!void {
    const t = trimSpan(text, start, end);
    if (t.end > t.start) try list.append(allocator, t);
}

// ---- Auto-extracted source.* facets --------------------------------------

pub const SourceFacetContext = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    /// Ontology that owns the `source` namespace; the per-workspace default
    /// ontology is the canonical choice (it is bootstrapped automatically
    /// in `ensureWorkspace`).
    ontology_id: []const u8,
    doc_id: u64,
    /// Optional canonical ingestion timestamp (ISO-8601). When `null` the
    /// derived facets simply omit the `ingested_at` row.
    ingested_at: ?[]const u8 = null,
    /// Origin of the document (file path, URL, opaque locator). When `null`
    /// the path/dir/filename/extension rows are skipped.
    source_ref: ?[]const u8 = null,
};

pub const SourceFacets = struct {
    rows: []collections.FacetAssignmentRawSpec,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *SourceFacets) void {
        self.arena.deinit();
    }
};

/// Builds the `source.*` facet rows for a single chunk. The returned arena
/// owns every string referenced by the produced specs, so the caller can
/// freely persist them and then call `SourceFacets.deinit`.
pub fn deriveSourceFacets(
    allocator: std.mem.Allocator,
    ctx: SourceFacetContext,
    chunk_value: Chunk,
    total_chunks: u32,
) Error!SourceFacets {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var rows = std.ArrayList(collections.FacetAssignmentRawSpec).empty;
    errdefer rows.deinit(arena_alloc);

    if (ctx.source_ref) |raw_ref| {
        const ref_path = stripUriScheme(raw_ref);
        const path_value = try arena_alloc.dupe(u8, ref_path);
        try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "path", path_value));

        const filename = basename(ref_path);
        if (filename.len > 0) {
            const filename_value = try arena_alloc.dupe(u8, filename);
            try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "filename", filename_value));
        }

        const ext = lowercaseExtension(arena_alloc, filename) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
        };
        if (ext.len > 0) {
            try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "extension", ext));
        }

        const dir_components = try splitDirectoryComponents(arena_alloc, ref_path);
        for (dir_components) |dir_value| {
            try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "dir", dir_value));
        }
    }

    if (ctx.ingested_at) |ts| {
        const ts_value = try arena_alloc.dupe(u8, ts);
        try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "ingested_at", ts_value));
    }

    {
        const idx_value = try std.fmt.allocPrint(arena_alloc, "{d}", .{chunk_value.index});
        try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "chunk_index", idx_value));
    }
    {
        const total_value = try std.fmt.allocPrint(arena_alloc, "{d}", .{total_chunks});
        try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "chunk_count", total_value));
    }
    {
        const strategy_value = try arena_alloc.dupe(u8, chunk_value.strategy);
        try rows.append(arena_alloc, makeChunkFacet(ctx, chunk_value, "strategy", strategy_value));
    }

    return .{
        .rows = try rows.toOwnedSlice(arena_alloc),
        .arena = arena,
    };
}

fn makeChunkFacet(
    ctx: SourceFacetContext,
    chunk_value: Chunk,
    dimension: []const u8,
    value: []const u8,
) collections.FacetAssignmentRawSpec {
    return .{
        .workspace_id = ctx.workspace_id,
        .collection_id = ctx.collection_id,
        .target_kind = .chunk,
        .doc_id = ctx.doc_id,
        .chunk_index = chunk_value.index,
        .ontology_id = ctx.ontology_id,
        .namespace = collections.source_namespace,
        .dimension = dimension,
        .value = value,
        .source = "chunker.deriveSourceFacets",
    };
}

fn stripUriScheme(ref: []const u8) []const u8 {
    if (std.mem.indexOf(u8, ref, "://")) |scheme_end| {
        const after = ref[scheme_end + 3 ..];
        // Drop the authority component for http(s) so that the path-derived
        // facets stay focused on the resource itself.
        if (std.mem.indexOfScalar(u8, after, '/')) |slash| return after[slash..];
        return after;
    }
    return ref;
}

fn basename(path: []const u8) []const u8 {
    var trimmed = path;
    while (trimmed.len > 0 and trimmed[trimmed.len - 1] == '/') : (trimmed = trimmed[0 .. trimmed.len - 1]) {}
    if (std.mem.lastIndexOfScalar(u8, trimmed, '/')) |slash| return trimmed[slash + 1 ..];
    return trimmed;
}

fn lowercaseExtension(allocator: std.mem.Allocator, filename: []const u8) std.mem.Allocator.Error![]const u8 {
    if (filename.len == 0) return "";
    const dot = std.mem.lastIndexOfScalar(u8, filename, '.') orelse return "";
    if (dot == 0 or dot + 1 >= filename.len) return "";
    const ext = filename[dot + 1 ..];
    const buf = try allocator.alloc(u8, ext.len);
    for (ext, 0..) |b, i| buf[i] = std.ascii.toLower(b);
    return buf;
}

fn splitDirectoryComponents(allocator: std.mem.Allocator, path: []const u8) Error![][]const u8 {
    var components = std.ArrayList([]const u8).empty;
    errdefer components.deinit(allocator);

    // Anything after the last '/' is the filename — drop it.
    const last_slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse return components.toOwnedSlice(allocator);
    if (last_slash == 0) return components.toOwnedSlice(allocator);

    const dir_part = path[0..last_slash];
    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(allocator);

    var iter = std.mem.splitScalar(u8, dir_part, '/');
    var first = true;
    while (iter.next()) |segment| {
        if (segment.len == 0) continue;
        if (!first) try prefix.append(allocator, '/');
        try prefix.appendSlice(allocator, segment);
        first = false;
        const snapshot = try allocator.dupe(u8, prefix.items);
        try components.append(allocator, snapshot);
    }

    return components.toOwnedSlice(allocator);
}

// ---- Embedding-aware strategies ------------------------------------------

/// Callback that embeds an ordered batch of sentence-shaped strings. Used by
/// the semantic chunker. `out_embeddings.len == sentences.len`. The returned
/// slice and inner vectors are owned by the allocator passed to
/// `chunkSemantic` (the chunker frees them).
pub const EmbedSentencesFn = *const fn (
    allocator: std.mem.Allocator,
    sentences: []const []const u8,
    user_data: ?*anyopaque,
) anyerror![][]f32;

/// Callback that embeds a full document and returns a single contextualized
/// vector. Used by the late chunker so the pipeline can persist a parent
/// "context" embedding alongside the per-slice rows.
pub const EmbedFullDocFn = *const fn (
    allocator: std.mem.Allocator,
    text: []const u8,
    user_data: ?*anyopaque,
) anyerror![]f32;

pub const SemanticOptions = struct {
    embed: EmbedSentencesFn,
    user_data: ?*anyopaque = null,
    /// Cosine-similarity threshold below which two adjacent sentences are
    /// considered topic-distinct enough to start a new chunk. Range
    /// [-1.0, 1.0]; defaults are tuned for sentence-transformer style models.
    similarity_threshold: f32 = 0.6,
    /// Hard ceiling on chunk size in bytes; even when sentences stay
    /// topically related the chunker will flush once this is reached so a
    /// downstream LLM context window is respected.
    max_chars: usize = 2048,
    /// Hard ceiling on number of sentences per chunk; protects against
    /// degenerate cases where every adjacent similarity is above threshold.
    max_sentences_per_chunk: u32 = 32,
};

pub const LateOptions = struct {
    embed: EmbedFullDocFn,
    user_data: ?*anyopaque = null,
    /// Token target for the per-slice rows produced underneath the
    /// document-level parent.
    target_tokens: u32 = 256,
    /// Token overlap between adjacent slices.
    overlap_tokens: u32 = 32,
};

pub const LateResult = struct {
    /// First entry (index 0) is the whole-document parent row whose
    /// `parent_chunk_index` is `null`; subsequent rows are slices with
    /// `parent_chunk_index = 0`.
    chunks: []Chunk,
    /// Contextualized whole-document embedding produced by `LateOptions.embed`.
    /// Same allocator as the call to `chunkLate`.
    document_embedding: []f32,

    pub fn deinit(self: *LateResult, allocator: std.mem.Allocator) void {
        allocator.free(self.chunks);
        allocator.free(self.document_embedding);
    }
};

pub fn chunkSemantic(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: SemanticOptions,
) anyerror![]Chunk {
    if (text.len == 0) return allocator.alloc(Chunk, 0);

    const spans = try collectSentenceSpans(allocator, text);
    defer allocator.free(spans);
    if (spans.len == 0) return allocator.alloc(Chunk, 0);

    var sentences = try allocator.alloc([]const u8, spans.len);
    defer allocator.free(sentences);
    for (spans, 0..) |span, i| sentences[i] = text[span.start..span.end];

    const embeddings = try options.embed(allocator, sentences, options.user_data);
    defer freeEmbeddings(allocator, embeddings);

    if (embeddings.len != spans.len) return error.InvalidOptions;

    var chunks = std.ArrayList(Chunk).empty;
    errdefer chunks.deinit(allocator);

    var index: u32 = 0;
    var group_start: usize = 0;
    var i: usize = 1;
    while (i <= spans.len) : (i += 1) {
        const close = blk: {
            if (i == spans.len) break :blk true;
            const sim = cosineSimilarity(embeddings[i - 1], embeddings[i]);
            if (sim < options.similarity_threshold) break :blk true;
            const accumulated = spans[i - 1].end - spans[group_start].start;
            if (accumulated > options.max_chars) break :blk true;
            if ((i - group_start) >= options.max_sentences_per_chunk) break :blk true;
            break :blk false;
        };
        if (close) {
            const start_byte = spans[group_start].start;
            const end_byte = spans[i - 1].end;
            const slice = text[start_byte..end_byte];
            try chunks.append(allocator, .{
                .index = index,
                .content = slice,
                .offset_start = start_byte,
                .offset_end = end_byte,
                .token_count = countTokens(slice),
                .strategy = Strategy.semantic.label(),
            });
            index += 1;
            group_start = i;
        }
    }

    return chunks.toOwnedSlice(allocator);
}

pub fn chunkLate(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: LateOptions,
) anyerror!LateResult {
    if (text.len == 0) {
        return .{
            .chunks = try allocator.alloc(Chunk, 0),
            .document_embedding = try allocator.alloc(f32, 0),
        };
    }

    const document_embedding = try options.embed(allocator, text, options.user_data);
    errdefer allocator.free(document_embedding);

    const slices = try chunkFixedToken(allocator, text, .{
        .strategy = .fixed_token,
        .target_tokens = options.target_tokens,
        .overlap_tokens = options.overlap_tokens,
    });
    defer allocator.free(slices);

    var chunks = try allocator.alloc(Chunk, slices.len + 1);
    errdefer allocator.free(chunks);

    chunks[0] = .{
        .index = 0,
        .content = text,
        .offset_start = 0,
        .offset_end = text.len,
        .token_count = countTokens(text),
        .parent_chunk_index = null,
        .strategy = Strategy.late.label(),
    };
    var idx: u32 = 1;
    for (slices) |sc| {
        chunks[idx] = .{
            .index = idx,
            .content = sc.content,
            .offset_start = sc.offset_start,
            .offset_end = sc.offset_end,
            .token_count = sc.token_count,
            .parent_chunk_index = 0,
            .strategy = Strategy.late.label(),
        };
        idx += 1;
    }

    return .{
        .chunks = chunks,
        .document_embedding = document_embedding,
    };
}

fn freeEmbeddings(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |emb| allocator.free(emb);
    allocator.free(embeddings);
}

fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
    const len = @min(a.len, b.len);
    if (len == 0) return 0;
    var dot: f32 = 0;
    var na: f32 = 0;
    var nb: f32 = 0;
    var i: usize = 0;
    while (i < len) : (i += 1) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if (na == 0 or nb == 0) return 0;
    return dot / (std.math.sqrt(na) * std.math.sqrt(nb));
}

// ---- Tests ---------------------------------------------------------------

test "countTokens counts whitespace/punctuation-separated words" {
    try std.testing.expectEqual(@as(u64, 0), countTokens(""));
    try std.testing.expectEqual(@as(u64, 0), countTokens("   \n\t"));
    try std.testing.expectEqual(@as(u64, 4), countTokens("the quick brown fox"));
    // "It's" splits on the apostrophe into "It" + "s", which is the
    // intended behavior for budgeting LLM tokens.
    try std.testing.expectEqual(@as(u64, 5), countTokens("Hello, World! It's me."));
}

test "fixed_token chunking respects target and overlap" {
    const text = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
    const chunks = try chunk(std.testing.allocator, text, .{
        .strategy = .fixed_token,
        .target_tokens = 4,
        .overlap_tokens = 1,
    });
    defer freeChunks(std.testing.allocator, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expectEqualStrings("alpha beta gamma delta", chunks[0].content);
    try std.testing.expectEqualStrings("delta epsilon zeta eta", chunks[1].content);
    try std.testing.expectEqualStrings("eta theta iota kappa", chunks[2].content);
    for (chunks) |ch| try std.testing.expectEqualStrings("fixed_token", ch.strategy);
    try std.testing.expectEqual(@as(u64, 4), chunks[0].token_count);
}

test "fixed_token rejects nonsense overlap configurations" {
    try std.testing.expectError(error.InvalidOptions, chunk(std.testing.allocator, "alpha beta", .{
        .strategy = .fixed_token,
        .target_tokens = 2,
        .overlap_tokens = 2,
    }));
    try std.testing.expectError(error.InvalidOptions, chunk(std.testing.allocator, "alpha beta", .{
        .strategy = .fixed_token,
        .target_tokens = 0,
    }));
}

test "paragraph chunking splits on blank lines and trims whitespace" {
    const text = "First paragraph.\n\nSecond paragraph spans\n one line break only.\n\n   \n\nThird.";
    const chunks = try chunk(std.testing.allocator, text, .{ .strategy = .paragraph });
    defer freeChunks(std.testing.allocator, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expectEqualStrings("First paragraph.", chunks[0].content);
    try std.testing.expectEqualStrings("Second paragraph spans\n one line break only.", chunks[1].content);
    try std.testing.expectEqualStrings("Third.", chunks[2].content);
    try std.testing.expectEqualStrings("paragraph", chunks[0].strategy);
}

test "sentence chunking groups under max_chars" {
    const text = "First. Second sentence here! And a third? Fourth.";
    const chunks = try chunk(std.testing.allocator, text, .{
        .strategy = .sentence,
        .max_chars = 24,
    });
    defer freeChunks(std.testing.allocator, chunks);

    try std.testing.expect(chunks.len >= 2);
    var rebuilt = std.ArrayList(u8).empty;
    defer rebuilt.deinit(std.testing.allocator);
    for (chunks, 0..) |ch, i| {
        if (i > 0) try rebuilt.append(std.testing.allocator, ' ');
        try rebuilt.appendSlice(std.testing.allocator, ch.content);
        try std.testing.expect(ch.content.len <= 24);
    }
}

test "recursive_character keeps every chunk under max_chars" {
    const text =
        "Section one talks about graphs.\n\nSection two introduces facets and bitmaps."
        ++ " It deliberately gets longer so the splitter has to recurse past the\n"
        ++ "paragraph separator down to sentences and words.";
    const max: usize = 64;
    const chunks = try chunk(std.testing.allocator, text, .{
        .strategy = .recursive_character,
        .max_chars = max,
    });
    defer freeChunks(std.testing.allocator, chunks);

    try std.testing.expect(chunks.len > 1);
    for (chunks) |ch| try std.testing.expect(ch.content.len <= max);

    // Concatenating the pieces back recovers the meaningful content
    // (modulo whitespace at boundaries).
    var rebuilt = std.ArrayList(u8).empty;
    defer rebuilt.deinit(std.testing.allocator);
    for (chunks) |ch| try rebuilt.appendSlice(std.testing.allocator, ch.content);
    try std.testing.expect(std.mem.indexOf(u8, rebuilt.items, "facets and bitmaps") != null);
}

test "structure_aware keeps headings and code fences as standalone sections" {
    const text =
        \\# Title
        \\Intro paragraph.
        \\
        \\## Subtitle
        \\Body of the subsection.
        \\
        \\```zig
        \\const x = 42;
        \\```
        \\
        \\Trailing prose.
    ;
    const chunks = try chunk(std.testing.allocator, text, .{
        .strategy = .structure_aware,
        .max_chars = 4096,
    });
    defer freeChunks(std.testing.allocator, chunks);

    try std.testing.expect(chunks.len >= 3);
    try std.testing.expect(std.mem.startsWith(u8, chunks[0].content, "# Title"));
    var saw_fence = false;
    for (chunks) |ch| {
        if (std.mem.indexOf(u8, ch.content, "```zig") != null) saw_fence = true;
    }
    try std.testing.expect(saw_fence);
}

test "empty input yields zero chunks" {
    const chunks = try chunk(std.testing.allocator, "", .{ .strategy = .recursive_character });
    defer freeChunks(std.testing.allocator, chunks);
    try std.testing.expectEqual(@as(usize, 0), chunks.len);
}

// ---- deriveSourceFacets tests --------------------------------------------

fn findFacetByDimension(rows: []collections.FacetAssignmentRawSpec, dim: []const u8) ?collections.FacetAssignmentRawSpec {
    for (rows) |row| {
        if (std.mem.eql(u8, row.dimension, dim)) return row;
    }
    return null;
}

fn countFacetsByDimension(rows: []collections.FacetAssignmentRawSpec, dim: []const u8) usize {
    var count: usize = 0;
    for (rows) |row| {
        if (std.mem.eql(u8, row.dimension, dim)) count += 1;
    }
    return count;
}

test "deriveSourceFacets emits canonical source.* rows for filesystem paths" {
    const sample_chunk = Chunk{
        .index = 3,
        .content = "ignored",
        .offset_start = 0,
        .offset_end = 7,
        .token_count = 1,
        .strategy = Strategy.fixed_token.label(),
    };
    var facets = try deriveSourceFacets(std.testing.allocator, .{
        .workspace_id = "wsX",
        .collection_id = "wsX::docs",
        .ontology_id = "wsX::default",
        .doc_id = 99,
        .ingested_at = "2026-04-21T10:00:00Z",
        .source_ref = "/data/legal/contracts/2026/Final.PDF",
    }, sample_chunk, 12);
    defer facets.deinit();

    const path_row = findFacetByDimension(facets.rows, "path") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("/data/legal/contracts/2026/Final.PDF", path_row.value);
    try std.testing.expect(path_row.target_kind == .chunk);
    try std.testing.expectEqual(@as(?u32, 3), path_row.chunk_index);
    try std.testing.expectEqualStrings("source", path_row.namespace);

    const filename_row = findFacetByDimension(facets.rows, "filename") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("Final.PDF", filename_row.value);

    const ext_row = findFacetByDimension(facets.rows, "extension") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("pdf", ext_row.value);

    try std.testing.expectEqual(@as(usize, 4), countFacetsByDimension(facets.rows, "dir"));
    const dir_values = [_][]const u8{ "data", "data/legal", "data/legal/contracts", "data/legal/contracts/2026" };
    var matched: usize = 0;
    for (facets.rows) |row| {
        if (!std.mem.eql(u8, row.dimension, "dir")) continue;
        for (dir_values) |expected| {
            if (std.mem.eql(u8, row.value, expected)) matched += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, dir_values.len), matched);

    const ingested_row = findFacetByDimension(facets.rows, "ingested_at") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("2026-04-21T10:00:00Z", ingested_row.value);

    const idx_row = findFacetByDimension(facets.rows, "chunk_index") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("3", idx_row.value);
    const total_row = findFacetByDimension(facets.rows, "chunk_count") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("12", total_row.value);
    const strategy_row = findFacetByDimension(facets.rows, "strategy") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("fixed_token", strategy_row.value);
}

test "deriveSourceFacets strips the URI authority before splitting" {
    const sample_chunk = Chunk{
        .index = 0,
        .content = "ignored",
        .offset_start = 0,
        .offset_end = 7,
        .token_count = 1,
        .strategy = Strategy.recursive_character.label(),
    };
    var facets = try deriveSourceFacets(std.testing.allocator, .{
        .workspace_id = "ws",
        .collection_id = "ws::docs",
        .ontology_id = "ws::default",
        .doc_id = 1,
        .source_ref = "https://example.com/articles/2026/04/index.html",
    }, sample_chunk, 1);
    defer facets.deinit();

    const path_row = findFacetByDimension(facets.rows, "path") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("/articles/2026/04/index.html", path_row.value);
    const ext_row = findFacetByDimension(facets.rows, "extension") orelse return error.MissingFacet;
    try std.testing.expectEqualStrings("html", ext_row.value);
    try std.testing.expectEqual(@as(usize, 3), countFacetsByDimension(facets.rows, "dir"));
}

test "deriveSourceFacets handles missing source_ref gracefully" {
    const sample_chunk = Chunk{
        .index = 0,
        .content = "ignored",
        .offset_start = 0,
        .offset_end = 7,
        .token_count = 1,
        .strategy = Strategy.paragraph.label(),
    };
    var facets = try deriveSourceFacets(std.testing.allocator, .{
        .workspace_id = "ws",
        .collection_id = "ws::docs",
        .ontology_id = "ws::default",
        .doc_id = 1,
    }, sample_chunk, 1);
    defer facets.deinit();

    try std.testing.expectEqual(@as(usize, 0), countFacetsByDimension(facets.rows, "path"));
    try std.testing.expectEqual(@as(usize, 0), countFacetsByDimension(facets.rows, "dir"));
    try std.testing.expect(findFacetByDimension(facets.rows, "chunk_index") != null);
    try std.testing.expect(findFacetByDimension(facets.rows, "strategy") != null);
}

// ---- Embedding-aware tests -----------------------------------------------

const StubSemanticState = struct {
    /// Map "first word of sentence" → cluster id. All sentences in the same
    /// cluster get a unit-norm vector along the cluster axis, so cosine
    /// similarity is 1.0 within a cluster and 0.0 across clusters.
    clusters: []const []const u8,
};

fn stubEmbedSentences(
    allocator: std.mem.Allocator,
    sentences: []const []const u8,
    user_data: ?*anyopaque,
) anyerror![][]f32 {
    const state: *const StubSemanticState = @ptrCast(@alignCast(user_data orelse return error.InvalidOptions));
    var out = try allocator.alloc([]f32, sentences.len);
    errdefer {
        for (out) |emb| allocator.free(emb);
        allocator.free(out);
    }
    for (sentences, 0..) |s, i| {
        const vec = try allocator.alloc(f32, state.clusters.len);
        @memset(vec, 0);
        var matched: bool = false;
        for (state.clusters, 0..) |key, axis| {
            if (std.mem.startsWith(u8, s, key)) {
                vec[axis] = 1.0;
                matched = true;
                break;
            }
        }
        if (!matched) vec[0] = 1.0;
        out[i] = vec;
    }
    return out;
}

test "semantic chunker splits at low cross-cluster similarity" {
    const text =
        "Cats love naps in the sun. Cats are curious creatures." ++
        " Dogs run around the park. Dogs bark loudly at strangers." ++
        " Birds build nests on tall trees. Birds sing in the morning.";
    const state = StubSemanticState{
        .clusters = &.{ "Cats", "Dogs", "Birds" },
    };
    const chunks = try chunkSemantic(std.testing.allocator, text, .{
        .embed = stubEmbedSentences,
        .user_data = @ptrCast(@constCast(&state)),
        .similarity_threshold = 0.5,
    });
    defer freeChunks(std.testing.allocator, chunks);

    try std.testing.expectEqual(@as(usize, 3), chunks.len);
    try std.testing.expect(std.mem.startsWith(u8, chunks[0].content, "Cats"));
    try std.testing.expect(std.mem.startsWith(u8, chunks[1].content, "Dogs"));
    try std.testing.expect(std.mem.startsWith(u8, chunks[2].content, "Birds"));
    for (chunks) |ch| try std.testing.expectEqualStrings("semantic", ch.strategy);
}

test "semantic chunker honours max_sentences_per_chunk" {
    const text = "Cats one. Cats two. Cats three. Cats four. Cats five.";
    const state = StubSemanticState{ .clusters = &.{"Cats"} };
    const chunks = try chunkSemantic(std.testing.allocator, text, .{
        .embed = stubEmbedSentences,
        .user_data = @ptrCast(@constCast(&state)),
        .similarity_threshold = 0.0,
        .max_sentences_per_chunk = 2,
    });
    defer freeChunks(std.testing.allocator, chunks);
    try std.testing.expectEqual(@as(usize, 3), chunks.len);
}

const StubLateState = struct {
    dim: usize,
};

fn stubEmbedFullDoc(
    allocator: std.mem.Allocator,
    text: []const u8,
    user_data: ?*anyopaque,
) anyerror![]f32 {
    const state: *const StubLateState = @ptrCast(@alignCast(user_data orelse return error.InvalidOptions));
    const out = try allocator.alloc(f32, state.dim);
    @memset(out, 0);
    if (out.len > 0) out[0] = @floatFromInt(text.len);
    return out;
}

test "late chunker emits doc-level parent followed by token slices" {
    const text = "alpha beta gamma delta epsilon zeta eta theta iota kappa";
    const state = StubLateState{ .dim = 4 };
    var result = try chunkLate(std.testing.allocator, text, .{
        .embed = stubEmbedFullDoc,
        .user_data = @ptrCast(@constCast(&state)),
        .target_tokens = 4,
        .overlap_tokens = 1,
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 4), result.document_embedding.len);
    try std.testing.expectEqual(@as(f32, @floatFromInt(text.len)), result.document_embedding[0]);

    try std.testing.expect(result.chunks.len >= 2);
    try std.testing.expectEqual(@as(?u32, null), result.chunks[0].parent_chunk_index);
    try std.testing.expectEqualStrings(text, result.chunks[0].content);
    for (result.chunks[1..]) |child| {
        try std.testing.expectEqual(@as(?u32, 0), child.parent_chunk_index);
        try std.testing.expectEqualStrings("late", child.strategy);
    }
}
