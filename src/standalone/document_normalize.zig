//! Converts external document formats into text/Markdown before profiling.
//!
//! Heavy extractors remain external processes. This module orchestrates them
//! and records enough metadata for the profile/chunk pipeline to audit inputs.

const std = @import("std");

pub const DocumentKind = enum {
    html,
    pdf,
    text,
    unknown,

    pub fn label(self: DocumentKind) []const u8 {
        return switch (self) {
            .html => "html",
            .pdf => "pdf",
            .text => "text",
            .unknown => "unknown",
        };
    }
};

pub const PdfBackend = enum {
    auto,
    pdftotext,
    ocrmypdf,
    deepseek,
    none,

    pub fn label(self: PdfBackend) []const u8 {
        return switch (self) {
            .auto => "auto",
            .pdftotext => "pdftotext",
            .ocrmypdf => "ocrmypdf",
            .deepseek => "deepseek",
            .none => "none",
        };
    }
};

pub const HtmlBackend = enum {
    pandoc,
    builtin_strip,

    pub fn label(self: HtmlBackend) []const u8 {
        return switch (self) {
            .pandoc => "pandoc",
            .builtin_strip => "builtin_strip",
        };
    }
};

pub const Options = struct {
    input_path: []const u8,
    output_dir: []const u8,
    languages: []const []const u8 = &.{},
    split_by_language: bool = false,
    pdf_backend: PdfBackend = .auto,
    html_backend: HtmlBackend = .pandoc,
    deepseek_command: ?[]const u8 = null,
    min_text_chars: usize = 128,
    max_process_output_bytes: usize = 128 * 1024 * 1024,
};

pub const Output = struct {
    path: []u8,
    metadata_path: []u8,
    language: []u8,
    extractor: []u8,
    needs_language_split: bool,
};

pub const Result = struct {
    input_path: []u8,
    manifest_path: []u8,
    kind: DocumentKind,
    outputs: []Output,

    pub fn deinit(self: *Result, allocator: std.mem.Allocator) void {
        allocator.free(self.input_path);
        allocator.free(self.manifest_path);
        for (self.outputs) |out| {
            allocator.free(out.path);
            allocator.free(out.metadata_path);
            allocator.free(out.language);
            allocator.free(out.extractor);
        }
        allocator.free(self.outputs);
    }
};

const Extracted = struct {
    text: []u8,
    extractor: []u8,
};

const LangText = struct {
    language: []const u8,
    text: []u8,
    needs_language_split: bool = false,
};

pub fn normalize(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
) !Result {
    try std.Io.Dir.cwd().createDirPath(io, opts.output_dir);

    const kind = detectKind(opts.input_path);
    const extracted = try extract(allocator, io, kind, opts);
    defer allocator.free(extracted.text);
    defer allocator.free(extracted.extractor);

    const pieces = try splitForLanguages(allocator, extracted.text, opts.languages, opts.split_by_language);
    defer {
        for (pieces) |piece| allocator.free(piece.text);
        allocator.free(pieces);
    }

    var outputs = std.ArrayList(Output).empty;
    errdefer {
        for (outputs.items) |out| {
            allocator.free(out.path);
            allocator.free(out.metadata_path);
            allocator.free(out.language);
            allocator.free(out.extractor);
        }
        outputs.deinit(allocator);
    }

    const stem = try outputStem(allocator, opts.input_path);
    defer allocator.free(stem);
    const extension = outputExtension(kind, extracted.extractor);

    for (pieces) |piece| {
        const suffix = if (std.mem.eql(u8, piece.language, "und")) "" else piece.language;
        const filename = if (suffix.len == 0)
            try std.fmt.allocPrint(allocator, "{s}{s}", .{ stem, extension })
        else
            try std.fmt.allocPrint(allocator, "{s}-{s}{s}", .{ stem, suffix, extension });
        defer allocator.free(filename);

        const output_path = try std.fs.path.join(allocator, &.{ opts.output_dir, filename });
        errdefer allocator.free(output_path);
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = output_path,
            .data = piece.text,
            .flags = .{ .truncate = true },
        });

        const metadata_path = try std.fmt.allocPrint(allocator, "{s}.metadata.json", .{output_path});
        errdefer allocator.free(metadata_path);
        const metadata = try metadataJson(allocator, .{
            .source_path = opts.input_path,
            .output_path = output_path,
            .kind = kind.label(),
            .extractor = extracted.extractor,
            .language = piece.language,
            .split_by_language = opts.split_by_language,
            .needs_language_split = piece.needs_language_split,
        });
        defer allocator.free(metadata);
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = metadata_path,
            .data = metadata,
            .flags = .{ .truncate = true },
        });

        try outputs.append(allocator, .{
            .path = output_path,
            .metadata_path = metadata_path,
            .language = try allocator.dupe(u8, piece.language),
            .extractor = try allocator.dupe(u8, extracted.extractor),
            .needs_language_split = piece.needs_language_split,
        });
    }

    const manifest_path = try std.fs.path.join(allocator, &.{ opts.output_dir, "manifest.json" });
    errdefer allocator.free(manifest_path);
    const manifest = try manifestJson(allocator, opts.input_path, kind, outputs.items);
    defer allocator.free(manifest);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = manifest_path,
        .data = manifest,
        .flags = .{ .truncate = true },
    });

    return .{
        .input_path = try allocator.dupe(u8, opts.input_path),
        .manifest_path = manifest_path,
        .kind = kind,
        .outputs = try outputs.toOwnedSlice(allocator),
    };
}

fn extract(
    allocator: std.mem.Allocator,
    io: std.Io,
    kind: DocumentKind,
    opts: Options,
) !Extracted {
    return switch (kind) {
        .html => extractHtml(allocator, io, opts),
        .pdf => extractPdf(allocator, io, opts),
        .text, .unknown => .{
            .text = try std.Io.Dir.cwd().readFileAlloc(io, opts.input_path, allocator, .unlimited),
            .extractor = try allocator.dupe(u8, "copy"),
        },
    };
}

fn extractHtml(allocator: std.mem.Allocator, io: std.Io, opts: Options) !Extracted {
    switch (opts.html_backend) {
        .pandoc => {
            const result = runProcess(allocator, io, &.{ "pandoc", "-f", "html", "-t", "markdown", opts.input_path }, opts.max_process_output_bytes) catch |err| {
                if (err == error.FileNotFound) return extractHtmlBuiltin(allocator, io, opts);
                return err;
            };
            return .{ .text = result, .extractor = try allocator.dupe(u8, "pandoc") };
        },
        .builtin_strip => return extractHtmlBuiltin(allocator, io, opts),
    }
}

fn extractHtmlBuiltin(allocator: std.mem.Allocator, io: std.Io, opts: Options) !Extracted {
    const html = try std.Io.Dir.cwd().readFileAlloc(io, opts.input_path, allocator, .unlimited);
    defer allocator.free(html);
    return .{
        .text = try stripHtml(allocator, html),
        .extractor = try allocator.dupe(u8, "builtin_strip"),
    };
}

fn extractPdf(allocator: std.mem.Allocator, io: std.Io, opts: Options) !Extracted {
    if (opts.pdf_backend == .none) return error.UnsupportedExtractor;

    if (opts.pdf_backend == .pdftotext or opts.pdf_backend == .auto) {
        const text = runProcess(allocator, io, &.{ "pdftotext", "-layout", opts.input_path, "-" }, opts.max_process_output_bytes) catch |err| {
            if (opts.pdf_backend == .pdftotext) return err;
            return try extractPdfOcr(allocator, io, opts);
        };
        if (opts.pdf_backend == .pdftotext or usefulText(text, opts.min_text_chars)) {
            return .{ .text = text, .extractor = try allocator.dupe(u8, "pdftotext") };
        }
        allocator.free(text);
        return try extractPdfOcr(allocator, io, opts);
    }

    return try extractPdfOcr(allocator, io, opts);
}

fn extractPdfOcr(allocator: std.mem.Allocator, io: std.Io, opts: Options) !Extracted {
    return switch (opts.pdf_backend) {
        .deepseek => extractPdfDeepseek(allocator, io, opts),
        else => extractPdfOcrmypdf(allocator, io, opts),
    };
}

fn extractPdfOcrmypdf(allocator: std.mem.Allocator, io: std.Io, opts: Options) !Extracted {
    const stem = try outputStem(allocator, opts.input_path);
    defer allocator.free(stem);
    const sidecar_name = try std.fmt.allocPrint(allocator, "{s}.ocr.txt", .{stem});
    defer allocator.free(sidecar_name);
    const sidecar = try std.fs.path.join(allocator, &.{ opts.output_dir, sidecar_name });
    defer allocator.free(sidecar);
    const output_pdf_name = try std.fmt.allocPrint(allocator, "{s}.ocr.pdf", .{stem});
    defer allocator.free(output_pdf_name);
    const output_pdf = try std.fs.path.join(allocator, &.{ opts.output_dir, output_pdf_name });
    defer allocator.free(output_pdf);

    const language_arg = try ocrLanguageArg(allocator, opts.languages);
    defer allocator.free(language_arg);
    const argv = [_][]const u8{ "ocrmypdf", "-l", language_arg, "--sidecar", sidecar, "--output-type", "pdf", opts.input_path, output_pdf };
    const proc = try std.process.run(allocator, io, .{
        .argv = &argv,
        .stdout_limit = .limited(opts.max_process_output_bytes),
        .stderr_limit = .limited(opts.max_process_output_bytes),
    });
    defer allocator.free(proc.stdout);
    defer allocator.free(proc.stderr);
    if (!processSucceeded(proc.term)) return error.ExternalCommandFailed;

    return .{
        .text = try std.Io.Dir.cwd().readFileAlloc(io, sidecar, allocator, .unlimited),
        .extractor = try allocator.dupe(u8, "ocrmypdf"),
    };
}

fn extractPdfDeepseek(allocator: std.mem.Allocator, io: std.Io, opts: Options) !Extracted {
    const template = opts.deepseek_command orelse return error.UnsupportedExtractor;
    const output_text = try std.fs.path.join(allocator, &.{ opts.output_dir, "deepseek-output.md" });
    defer allocator.free(output_text);
    const command = try commandTemplate(allocator, template, opts.input_path, opts.output_dir, output_text);
    defer allocator.free(command);
    const argv = [_][]const u8{ "sh", "-c", command };
    const proc = try std.process.run(allocator, io, .{
        .argv = &argv,
        .stdout_limit = .limited(opts.max_process_output_bytes),
        .stderr_limit = .limited(opts.max_process_output_bytes),
    });
    defer allocator.free(proc.stdout);
    defer allocator.free(proc.stderr);
    if (!processSucceeded(proc.term)) return error.ExternalCommandFailed;
    return .{
        .text = try std.Io.Dir.cwd().readFileAlloc(io, output_text, allocator, .unlimited),
        .extractor = try allocator.dupe(u8, "deepseek"),
    };
}

fn runProcess(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8, limit: usize) ![]u8 {
    const proc = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(limit),
        .stderr_limit = .limited(limit),
    });
    defer allocator.free(proc.stderr);
    if (!processSucceeded(proc.term)) {
        allocator.free(proc.stdout);
        return error.ExternalCommandFailed;
    }
    return proc.stdout;
}

fn processSucceeded(term: std.process.Child.Term) bool {
    return switch (term) {
        .exited => |code| code == 0,
        else => false,
    };
}

pub fn detectKind(path: []const u8) DocumentKind {
    const ext = std.fs.path.extension(path);
    if (std.ascii.eqlIgnoreCase(ext, ".html") or std.ascii.eqlIgnoreCase(ext, ".htm")) return .html;
    if (std.ascii.eqlIgnoreCase(ext, ".pdf")) return .pdf;
    if (std.ascii.eqlIgnoreCase(ext, ".txt") or std.ascii.eqlIgnoreCase(ext, ".md") or std.ascii.eqlIgnoreCase(ext, ".markdown")) return .text;
    return .unknown;
}

fn outputExtension(kind: DocumentKind, extractor: []const u8) []const u8 {
    if (kind == .html) return ".md";
    if (std.mem.eql(u8, extractor, "deepseek")) return ".md";
    return ".txt";
}

pub fn outputStem(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const base = std.fs.path.basename(path);
    const ext = std.fs.path.extension(base);
    const raw = if (ext.len > 0) base[0 .. base.len - ext.len] else base;
    return sanitizeStem(allocator, raw);
}

fn sanitizeStem(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var previous_dash = false;
    for (raw) |ch| {
        const lower = std.ascii.toLower(ch);
        const keep = std.ascii.isAlphanumeric(lower);
        const next = if (keep) lower else '-';
        if (next == '-') {
            if (previous_dash) continue;
            previous_dash = true;
        } else {
            previous_dash = false;
        }
        try out.append(allocator, next);
    }
    while (out.items.len > 0 and out.items[out.items.len - 1] == '-') _ = out.pop();
    if (out.items.len == 0) try out.appendSlice(allocator, "document");
    return out.toOwnedSlice(allocator);
}

pub fn splitForLanguages(
    allocator: std.mem.Allocator,
    text: []const u8,
    languages: []const []const u8,
    split_by_language: bool,
) ![]LangText {
    if (!split_by_language or !hasLanguage(languages, "fr") or !hasLanguage(languages, "nl")) {
        const lang = if (languages.len == 1) languages[0] else "und";
        const out = try allocator.alloc(LangText, 1);
        out[0] = .{ .language = lang, .text = try allocator.dupe(u8, text), .needs_language_split = split_by_language and languages.len > 1 };
        return out;
    }
    return try splitFrenchDutch(allocator, text);
}

fn splitFrenchDutch(allocator: std.mem.Allocator, text: []const u8) ![]LangText {
    var fr = std.ArrayList(u8).empty;
    var nl = std.ArrayList(u8).empty;
    errdefer {
        fr.deinit(allocator);
        nl.deinit(allocator);
    }

    var pos: usize = 0;
    while (pos < text.len) {
        const line_end = std.mem.indexOfScalarPos(u8, text, pos, '\n') orelse text.len;
        const line = text[pos..line_end];
        const score = scoreFrenchDutch(line);
        if (score > 0) {
            try fr.appendSlice(allocator, line);
            try fr.append(allocator, '\n');
        } else if (score < 0) {
            try nl.appendSlice(allocator, line);
            try nl.append(allocator, '\n');
        } else {
            if (fr.items.len > nl.items.len) {
                try nl.appendSlice(allocator, line);
                try nl.append(allocator, '\n');
            } else {
                try fr.appendSlice(allocator, line);
                try fr.append(allocator, '\n');
            }
        }
        pos = if (line_end < text.len) line_end + 1 else text.len;
    }

    const fr_useful = usefulText(fr.items, 32);
    const nl_useful = usefulText(nl.items, 32);
    if (fr_useful and nl_useful) {
        const out = try allocator.alloc(LangText, 2);
        out[0] = .{ .language = "fr", .text = try fr.toOwnedSlice(allocator) };
        out[1] = .{ .language = "nl", .text = try nl.toOwnedSlice(allocator) };
        return out;
    }

    fr.deinit(allocator);
    nl.deinit(allocator);
    const out = try allocator.alloc(LangText, 1);
    out[0] = .{
        .language = "und",
        .text = try allocator.dupe(u8, text),
        .needs_language_split = true,
    };
    return out;
}

fn scoreFrenchDutch(line: []const u8) i32 {
    var fr: i32 = 0;
    var nl: i32 = 0;
    var it = std.mem.tokenizeAny(u8, line, " \t\r\n.,;:!?()[]{}\"'");
    while (it.next()) |word| {
        if (isFrenchWord(word)) fr += 1;
        if (isDutchWord(word)) nl += 1;
    }
    return fr - nl;
}

fn isFrenchWord(word: []const u8) bool {
    const words = [_][]const u8{ "le", "la", "les", "des", "du", "de", "et", "article", "arrete", "loi", "royal", "federal", "commune", "service", "est", "sont", "pour", "dans" };
    for (&words) |candidate| if (std.ascii.eqlIgnoreCase(word, candidate)) return true;
    return false;
}

fn isDutchWord(word: []const u8) bool {
    const words = [_][]const u8{ "de", "het", "een", "en", "artikel", "besluit", "wet", "koninklijk", "federaal", "gemeente", "dienst", "is", "zijn", "voor", "van", "in" };
    for (&words) |candidate| if (std.ascii.eqlIgnoreCase(word, candidate)) return true;
    return false;
}

fn hasLanguage(languages: []const []const u8, language: []const u8) bool {
    for (languages) |candidate| {
        if (std.ascii.eqlIgnoreCase(candidate, language)) return true;
    }
    return false;
}

fn usefulText(text: []const u8, minimum: usize) bool {
    var count: usize = 0;
    for (text) |ch| {
        if (!std.ascii.isWhitespace(ch)) count += 1;
        if (count >= minimum) return true;
    }
    return false;
}

fn stripHtml(allocator: std.mem.Allocator, html: []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var last_space = false;
    var pos: usize = 0;
    while (pos < html.len) : (pos += 1) {
        const ch = html[pos];
        if (ch == '<') {
            const tag_end = std.mem.indexOfScalarPos(u8, html, pos, '>') orelse html.len - 1;
            if (htmlTagAddsBreak(html[pos + 1 .. tag_end])) {
                if (out.items.len > 0 and out.items[out.items.len - 1] != '\n') {
                    try out.append(allocator, '\n');
                }
                last_space = true;
            } else if (!last_space) {
                try out.append(allocator, ' ');
                last_space = true;
            }
            pos = tag_end;
            continue;
        }
        if (std.ascii.isWhitespace(ch)) {
            if (!last_space) {
                try out.append(allocator, ' ');
                last_space = true;
            }
        } else {
            try out.append(allocator, ch);
            last_space = false;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn htmlTagAddsBreak(raw_tag: []const u8) bool {
    const tag = trimLeft(raw_tag, " /");
    return startsWithTag(tag, "p") or
        startsWithTag(tag, "br") or
        startsWithTag(tag, "div") or
        startsWithTag(tag, "li") or
        startsWithTag(tag, "h1") or
        startsWithTag(tag, "h2") or
        startsWithTag(tag, "h3") or
        startsWithTag(tag, "h4") or
        startsWithTag(tag, "h5") or
        startsWithTag(tag, "h6") or
        startsWithTag(tag, "tr");
}

fn trimLeft(value: []const u8, cutset: []const u8) []const u8 {
    var start: usize = 0;
    while (start < value.len and std.mem.indexOfScalar(u8, cutset, value[start]) != null) {
        start += 1;
    }
    return value[start..];
}

fn startsWithTag(tag: []const u8, name: []const u8) bool {
    if (tag.len < name.len) return false;
    if (!std.ascii.eqlIgnoreCase(tag[0..name.len], name)) return false;
    return tag.len == name.len or std.ascii.isWhitespace(tag[name.len]) or tag[name.len] == '>' or tag[name.len] == '/';
}

fn ocrLanguageArg(allocator: std.mem.Allocator, languages: []const []const u8) ![]u8 {
    if (languages.len == 0) return try allocator.dupe(u8, "eng");
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    for (languages, 0..) |lang, i| {
        if (i > 0) try out.append(allocator, '+');
        if (std.ascii.eqlIgnoreCase(lang, "fr")) try out.appendSlice(allocator, "fra") else if (std.ascii.eqlIgnoreCase(lang, "nl")) try out.appendSlice(allocator, "nld") else try out.appendSlice(allocator, lang);
    }
    return out.toOwnedSlice(allocator);
}

fn commandTemplate(
    allocator: std.mem.Allocator,
    template: []const u8,
    input_path: []const u8,
    output_dir: []const u8,
    output_text: []const u8,
) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var pos: usize = 0;
    while (pos < template.len) {
        if (std.mem.startsWith(u8, template[pos..], "{input}")) {
            try out.appendSlice(allocator, input_path);
            pos += "{input}".len;
        } else if (std.mem.startsWith(u8, template[pos..], "{output_dir}")) {
            try out.appendSlice(allocator, output_dir);
            pos += "{output_dir}".len;
        } else if (std.mem.startsWith(u8, template[pos..], "{output_text}")) {
            try out.appendSlice(allocator, output_text);
            pos += "{output_text}".len;
        } else {
            try out.append(allocator, template[pos]);
            pos += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

const MetadataJsonInput = struct {
    source_path: []const u8,
    output_path: []const u8,
    kind: []const u8,
    extractor: []const u8,
    language: []const u8,
    split_by_language: bool,
    needs_language_split: bool,
};

fn metadataJson(allocator: std.mem.Allocator, input: MetadataJsonInput) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, .{
        .source_path = input.source_path,
        .output_path = input.output_path,
        .source_kind = input.kind,
        .extractor = input.extractor,
        .language = input.language,
        .split_by_language = input.split_by_language,
        .needs_language_split = input.needs_language_split,
    }, .{ .whitespace = .indent_2 });
}

fn manifestJson(allocator: std.mem.Allocator, input_path: []const u8, kind: DocumentKind, outputs: []const Output) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, .{
        .input_path = input_path,
        .source_kind = kind.label(),
        .outputs = outputs,
    }, .{ .whitespace = .indent_2 });
}

test "detectKind recognizes common source formats" {
    try std.testing.expectEqual(DocumentKind.pdf, detectKind("a/b/file.PDF"));
    try std.testing.expectEqual(DocumentKind.html, detectKind("x.html"));
    try std.testing.expectEqual(DocumentKind.html, detectKind("x.HTM"));
    try std.testing.expectEqual(DocumentKind.text, detectKind("x.md"));
}

test "outputStem sanitizes file names" {
    const stem = try outputStem(std.testing.allocator, "/tmp/Moniteur Belge 2024-12-18.pdf");
    defer std.testing.allocator.free(stem);
    try std.testing.expectEqualStrings("moniteur-belge-2024-12-18", stem);
}

test "splitForLanguages separates simple French and Dutch lines" {
    const text =
        "Article 1. Le service federal est competent pour la commune.\n" ++
        "Artikel 1. De federale dienst is bevoegd voor de gemeente.\n" ++
        "Article 2. La loi est applicable dans la commune.\n" ++
        "Artikel 2. De wet is van toepassing in de gemeente.\n";
    const pieces = try splitForLanguages(std.testing.allocator, text, &.{ "fr", "nl" }, true);
    defer {
        for (pieces) |piece| std.testing.allocator.free(piece.text);
        std.testing.allocator.free(pieces);
    }
    try std.testing.expectEqual(@as(usize, 2), pieces.len);
    try std.testing.expectEqualStrings("fr", pieces[0].language);
    try std.testing.expectEqualStrings("nl", pieces[1].language);
    try std.testing.expect(std.mem.indexOf(u8, pieces[0].text, "Article 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, pieces[1].text, "Artikel 1") != null);
}

test "splitForLanguages falls back when split is ambiguous" {
    const text = "12345\n67890\n";
    const pieces = try splitForLanguages(std.testing.allocator, text, &.{ "fr", "nl" }, true);
    defer {
        for (pieces) |piece| std.testing.allocator.free(piece.text);
        std.testing.allocator.free(pieces);
    }
    try std.testing.expectEqual(@as(usize, 1), pieces.len);
    try std.testing.expectEqualStrings("und", pieces[0].language);
    try std.testing.expect(pieces[0].needs_language_split);
}
