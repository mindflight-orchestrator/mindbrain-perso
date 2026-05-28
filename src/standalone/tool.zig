const std = @import("std");
const mindbrain = @import("mindbrain");
const chunker = mindbrain.chunker;
const chunking_policy = mindbrain.chunking_policy;
const collections_io = mindbrain.collections_io;
const collections_sqlite = mindbrain.collections_sqlite;
const reindex_http = mindbrain.reindex_http;
const corpus_eval = mindbrain.corpus_eval;
const corpus_profile = mindbrain.corpus_profile;
const corpus_profile_prompt = mindbrain.corpus_profile_prompt;
const document_normalize = mindbrain.document_normalize;
const facet_sqlite = mindbrain.facet_sqlite;
const graph_diagnostics = mindbrain.graph_diagnostics;
const graph_sqlite = mindbrain.graph_sqlite;
const interfaces = mindbrain.interfaces;
const legal_chunker = mindbrain.legal_chunker;
const linkml_interchange = mindbrain.linkml_interchange;
const llm = mindbrain.llm;
const llm_provider = mindbrain.llm_provider;
const nanoid = mindbrain.nanoid;
const pragma_sqlite = mindbrain.pragma_sqlite;
const queue_sqlite = mindbrain.queue_sqlite;
const query_executor = mindbrain.query_executor;
const ontology_sqlite = mindbrain.ontology_sqlite;
const owl2_import = mindbrain.owl2_import;
const search_sqlite = mindbrain.search_sqlite;
const toon_exports = mindbrain.toon_exports;
const db_benchmark = mindbrain.db_benchmark;
const vector_blob = mindbrain.vector_blob;
const workspace_sqlite = mindbrain.workspace_sqlite;
const qualification_normalize = mindbrain.qualification_normalize;
const qualification_apply = mindbrain.qualification_apply;
const business_edge_normalize = mindbrain.business_edge_normalize;
const business_entity_normalize = mindbrain.business_entity_normalize;
const llm_context_budget = mindbrain.llm_context_budget;
const syndic_profile_seed = mindbrain.syndic_profile_seed;

const Allocator = std.mem.Allocator;

const CliError = error{
    InvalidArguments,
};

const BusinessReindexMode = enum { none, graph };

pub fn main(init: std.process.Init) !void {
    mindbrain.zig16_compat.setIo(init.io);
    const allocator = init.gpa;
    defer llm.http_client.clearLastHttpFailure();
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (args.len < 2) {
        try printUsage();
        return CliError.InvalidArguments;
    }

    if (std.mem.eql(u8, args[1], "traverse")) {
        try runTraverseCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "workspace-export")) {
        try runWorkspaceExportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "workspace-create")) {
        try runWorkspaceCreateCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "collection-create")) {
        try runCollectionCreateCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "ontology-register")) {
        try runOntologyRegisterCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "ontology-attach")) {
        try runOntologyAttachCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "ontology-import")) {
        try runOntologyImportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "ontology-export")) {
        try runOntologyExportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "ontology-compile-linkml")) {
        try runOntologyCompileLinkmlCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "ontology-export-linkml")) {
        try runOntologyExportLinkmlCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "qualification-vocab-list")) {
        try runQualificationVocabListCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-qualify")) {
        try runDocumentQualifyCommand(allocator, args[2..], init.environ_map);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-business-extract")) {
        try runDocumentBusinessExtractCommand(allocator, args[2..], init.environ_map);
        return;
    }

    if (std.mem.eql(u8, args[1], "backup-export")) {
        try runBackupExportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "backup-load")) {
        try runBackupLoadCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "collection-export")) {
        try runCollectionExportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "collection-import")) {
        try runCollectionImportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-ingest")) {
        try runDocumentIngestCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-by-nanoid")) {
        try runDocumentByNanoidCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-normalize")) {
        try runDocumentNormalizeCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-profile")) {
        try runDocumentProfileCommand(allocator, args[2..], init.environ_map);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-profile-enqueue")) {
        try runDocumentProfileEnqueueCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "document-profile-worker")) {
        try runDocumentProfileWorkerCommand(allocator, args[2..], init.environ_map);
        return;
    }

    if (std.mem.eql(u8, args[1], "contextual-search")) {
        try runContextualSearchCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "search-embedding-batch")) {
        try runSearchEmbeddingBatchCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "corpus-eval")) {
        try runCorpusEvalCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "external-link-add")) {
        try runExternalLinkAddCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "graph-path")) {
        try runGraphPathCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "graph-diagnostics")) {
        try runGraphDiagnosticsCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "graph-gap-rules-import")) {
        try runGraphGapRulesImportCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "search-compact-info")) {
        try runSearchCompactInfoCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "benchmark-db")) {
        try runBenchmarkDbCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "seed-demo")) {
        try runSeedDemoCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "bootstrap-from-sql")) {
        try runBootstrapFromSqlCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "workspace-export-by-domain")) {
        try runWorkspaceExportByDomainCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "coverage")) {
        try runCoverageCommand(allocator, args[2..], false);
        return;
    }

    if (std.mem.eql(u8, args[1], "coverage-by-domain")) {
        try runCoverageCommand(allocator, args[2..], true);
        return;
    }

    if (std.mem.eql(u8, args[1], "pack")) {
        try runPackCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "queue-send")) {
        try runQueueSendCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "queue-read")) {
        try runQueueReadCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "queue-archive")) {
        try runQueueArchiveCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "queue-delete")) {
        try runQueueDeleteCommand(allocator, args[2..]);
        return;
    }

    if (std.mem.eql(u8, args[1], "simulate")) {
        try runSimulationCommand(allocator, args[2..]);
        return;
    }

    try printUsage();
    return CliError.InvalidArguments;
}

fn runTraverseCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var start_name: ?[]const u8 = null;
    var direction: graph_sqlite.TraverseDirection = .outbound;
    var depth: usize = 3;
    var target_name: ?[]const u8 = null;
    var edge_labels = std.ArrayList([]const u8).empty;
    defer edge_labels.deinit(allocator);

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--start")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            start_name = args[index];
        } else if (std.mem.eql(u8, arg, "--direction")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            direction = parseDirection(args[index]) orelse return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--depth")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            depth = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--target")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            target_name = args[index];
        } else if (std.mem.eql(u8, arg, "--edge-label")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            try edge_labels.append(allocator, args[index]);
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or start_name == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    var result = try graph_sqlite.traverse(
        db,
        allocator,
        start_name.?,
        direction,
        if (edge_labels.items.len > 0) edge_labels.items else null,
        depth,
        target_name,
    );
    defer result.deinit(allocator);

    const payload = .{
        .target_found = result.target_found,
        .rows = result.rows,
    };
    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(payload, .{})});
    try stdout.flush();
}

fn parseDirection(value: []const u8) ?graph_sqlite.TraverseDirection {
    if (std.mem.eql(u8, value, "outbound")) return .outbound;
    if (std.mem.eql(u8, value, "inbound")) return .inbound;
    return null;
}

fn printUsage() !void {
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.writeAll(
        \\usage:
        \\  mindbrain-standalone-tool traverse --db <sqlite_path> --start <node_id> [--direction outbound|inbound] [--depth <n>] [--target <node_id>] [--edge-label <label> ...]
        \\  mindbrain-standalone-tool workspace-export --db <sqlite_path> --workspace-id <id>
        \\  mindbrain-standalone-tool workspace-create --db <sqlite_path> --workspace-id <id> [--label <text>] [--description <text>] [--profile <name>]
        \\  mindbrain-standalone-tool collection-create --db <sqlite_path> --workspace-id <id> --collection-id <id> --name <name> [--chunk-bits <n>] [--language <lang>]
        \\  mindbrain-standalone-tool ontology-register --db <sqlite_path> --workspace-id <id> --ontology-id <id> --name <name> [--version <v>] [--source-kind <kind>]
        \\  mindbrain-standalone-tool ontology-attach --db <sqlite_path> --workspace-id <id> --collection-id <id> --ontology-id <id> [--role <role>]
        \\  mindbrain-standalone-tool ontology-import --db <sqlite_path> --workspace-id <id> --ontology-id <id> --input <file.nt> [--name <name>] [--materialize-graph]
        \\  mindbrain-standalone-tool ontology-export --db <sqlite_path> --ontology-id <id> [--workspace-id <id> --format ntriples|bundle] [--output <file>]
        \\  mindbrain-standalone-tool ontology-compile-linkml --workspace-id <id> --ontology-id <id> --input <schema.yaml> [--output <bundle.json>] [--ntriples <file.nt>] [--db <sqlite_path>] [--name <name>]
        \\  mindbrain-standalone-tool ontology-export-linkml --ontology-id <id> (--db <sqlite_path> | --input-bundle <bundle.json>) [--output <schema.yaml>]
        \\  mindbrain-standalone-tool qualification-vocab-list --db <sqlite_path> --workspace-id <id> [--collection-id <id>] [--taxonomies <id,id>] [--facets <namespace.dimension,...>]
        \\  mindbrain-standalone-tool document-qualify --db <sqlite_path> --workspace-id <id> --collection-id <id> --taxonomies <id,id> --facets <namespace.dimension,...> ([--llm-provider openai|openrouter|anthropic] [--base-url <url>] [--model <name>] [--api-key <key>] | --mock-qualification-json <path> | --dry-run) [--limit <n>] [--target doc|chunk|both]
        \\  mindbrain-standalone-tool backup-export --db <sqlite_path> --workspace-id <id> [--scope workspace|taxonomies|collection] [--collection-id <id>] [--output <file>] [--no-vectors]
        \\  mindbrain-standalone-tool backup-load --db <sqlite_path> --bundle <file> [--dry-run] [--reindex none|graph|all] [--document-table-id N] [--collection-id <id>] [--table-id N]
        \\  mindbrain-standalone-tool collection-export --db <sqlite_path> --workspace-id <id> [--collection-id <id>] [--output <file>]
        \\  mindbrain-standalone-tool collection-import --db <sqlite_path> --bundle <file>
        \\  mindbrain-standalone-tool document-ingest --db <sqlite_path> --workspace-id <id> --collection-id <id> --doc-id <n> [--nanoid <id>] [--source-ref <uri>] [--language <lang>] [--ingested-at <iso>] [--ontology-id <id>] [--strategy fixed_token|sentence|paragraph|recursive_character|structure_aware] [--target-tokens <n>] [--overlap-tokens <n>] [--max-chars <n>] [--min-chars <n>] (--content <text> | --content-file <path>)
        \\  mindbrain-standalone-tool document-by-nanoid --db <sqlite_path> --nanoid <id>
        \\  mindbrain-standalone-tool document-normalize --input <path> --output-dir <dir> [--languages fr,nl] [--split-by-language] [--pdf-backend auto|pdftotext|ocrmypdf|deepseek|none] [--html-backend pandoc|builtin-strip] [--deepseek-command <template>]
        \\  mindbrain-standalone-tool document-profile (--content <text> | --content-file <path> | --content-dir <path>) ([--llm-provider openai|openrouter|anthropic] [--base-url <url>] [--model <name>] [--api-key <key>] | --mock-profile-json <path> | --dry-run) [--source-ref <ref>]
        \\  mindbrain-standalone-tool document-profile-enqueue --db <sqlite_path> (--content-file <path> | --content-dir <path>) [--queue <name>] [--include-ext md,txt] [--workspace-id <id> --collection-id <id> (--doc-id <n> | --doc-id-start <n>)] [--language <lang>]
        \\  mindbrain-standalone-tool document-profile-worker --db <sqlite_path> ([--llm-provider openai|openrouter|anthropic] [--base-url <url>] [--model <name>] [--api-key <key>] | --mock-profile-json <path>) [--queue <name>] [--vt <sec>] [--limit <n>] [--archive-failures] [--contextual-retrieval] [--contextual-doc-chars <n>] [--contextual-max-tokens <n>] [--contextual-search-table-id <n>] [--embedding-base-url <url>] [--embedding-api-key <key>] [--embedding-model <name>]
        \\  mindbrain-standalone-tool contextual-search --db <sqlite_path> --table-id <n> --query <text> [--base-url <url> --embedding-model <name> [--api-key <key>]] [--limit <n>] [--vector-weight <0..1>] [--rerank --rerank-base-url <url> --rerank-model <name> [--rerank-api-key <key>] [--rerank-candidates <n>] [--rerank-max-doc-chars <n>]]
        \\  mindbrain-standalone-tool search-embedding-batch --db <sqlite_path> --table-id <n> --embedding-base-url <url> --embedding-model <name> [--embedding-api-key <key>] [--limit <n>] [--missing-only]
        \\  mindbrain-standalone-tool corpus-eval [--fixtures <dir>] [--case <name>]
        \\  mindbrain-standalone-tool external-link-add --db <sqlite_path> --workspace-id <id> --source-collection-id <id> --source-doc-id <n> --target-uri <uri> [--source-chunk-index <n>] [--edge-type <name>] [--weight <float>] [--link-id <n>] [--metadata-json <json>]
        \\  mindbrain-standalone-tool graph-path --db <sqlite_path> --source <name> --target <name> [--edge-label <label> ...] [--max-depth <n>]
        \\  mindbrain-standalone-tool graph-diagnostics --db <sqlite_path> --workspace-id <id> [--ontology-id <id>] [--limit <n>] [--component-small-max <n>] [--format json|toon]
        \\  mindbrain-standalone-tool graph-gap-rules-import --db <sqlite_path> --input <rules.json>
        \\  mindbrain-standalone-tool search-compact-info --db <sqlite_path>
        \\  mindbrain-standalone-tool benchmark-db [--db <sqlite_path>] [--query-iterations <n>] [--mutation-iterations <n>]
        \\  mindbrain-standalone-tool seed-demo --db <sqlite_path>
        \\  mindbrain-standalone-tool bootstrap-from-sql --db <sqlite_path> --sql-file <path>
        \\  mindbrain-standalone-tool workspace-export-by-domain --db <sqlite_path> --domain-or-workspace <id>
        \\  mindbrain-standalone-tool coverage --db <sqlite_path> --workspace-id <id> [--entity-type <type> ...]
        \\  mindbrain-standalone-tool coverage-by-domain --db <sqlite_path> --domain-or-workspace <id> [--entity-type <type> ...]
        \\  mindbrain-standalone-tool pack --db <sqlite_path> --user-id <id> --query <text> [--scope <scope>] [--limit <n>]
        \\  mindbrain-standalone-tool queue-send --db <sqlite_path> --queue <name> --message <text>
        \\  mindbrain-standalone-tool queue-read --db <sqlite_path> --queue <name> [--vt <seconds>] [--limit <n>]
        \\  mindbrain-standalone-tool queue-archive --db <sqlite_path> --queue <name> --msg-id <id>
        \\  mindbrain-standalone-tool queue-delete --db <sqlite_path> --queue <name> --msg-id <id>
        \\  mindbrain-standalone-tool simulate
        \\
    );
    try stderr.flush();
}

fn runWorkspaceExportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--workspace-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            workspace_id = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or workspace_id == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const toon = try workspace_sqlite.exportWorkspaceModelToon(db, allocator, workspace_id.?);
    defer allocator.free(toon);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{s}\n", .{toon});
    try stdout.flush();
}

fn runSearchCompactInfoCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    if (args.len != 2) return CliError.InvalidArguments;
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const toon = try search_sqlite.compactSearchSnapshotToon(db, allocator);
    defer allocator.free(toon);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{s}\n", .{toon});
    try stdout.flush();
}

fn runBenchmarkDbCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: []const u8 = "data/imdb-full.sqlite";
    var query_iterations: usize = 25;
    var mutation_iterations: usize = 25;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--query-iterations")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            query_iterations = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--mutation-iterations")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            mutation_iterations = try std.fmt.parseInt(usize, args[index], 10);
        } else {
            return CliError.InvalidArguments;
        }
    }

    var db = try facet_sqlite.Database.open(db_path);
    defer db.close();
    try db.applyStandaloneSchema();

    var report = try db_benchmark.run(db, allocator, .{
        .query_iterations = query_iterations,
        .mutation_iterations = mutation_iterations,
    });
    defer report.deinit(allocator);

    const payload = .{
        .db_path = db_path,
        .graph_entity_count = report.graph_entity_count,
        .graph_relation_count = report.graph_relation_count,
        .graph_alias_count = report.graph_alias_count,
        .facet_query = report.facet_query,
        .graph_query = report.graph_query,
        .facet_single_add = report.facet_single_add,
        .facet_single_remove = report.facet_single_remove,
        .facet_single_update = report.facet_single_update,
        .facet_batch_update = report.facet_batch_update,
        .graph_single_add = report.graph_single_add,
        .graph_single_remove = report.graph_single_remove,
        .graph_single_update = report.graph_single_update,
        .graph_batch_update = report.graph_batch_update,
        .facet_toon = report.facet_toon,
        .graph_toon = report.graph_toon,
    };

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(payload, .{})});
    try stdout.flush();
}

fn runWorkspaceExportByDomainCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var domain_or_workspace: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--domain-or-workspace")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            domain_or_workspace = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or domain_or_workspace == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const toon = try workspace_sqlite.exportWorkspaceModelToonByDomain(db, allocator, domain_or_workspace.?);
    if (toon == null) return CliError.InvalidArguments;
    defer allocator.free(toon.?);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{s}\n", .{toon.?});
    try stdout.flush();
}

fn runGraphPathCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var source_name: ?[]const u8 = null;
    var target_name: ?[]const u8 = null;
    var edge_labels = std.ArrayList([]const u8).empty;
    defer edge_labels.deinit(allocator);
    var max_depth: usize = 4;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--source")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            source_name = args[index];
        } else if (std.mem.eql(u8, arg, "--target")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            target_name = args[index];
        } else if (std.mem.eql(u8, arg, "--edge-label")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            try edge_labels.append(allocator, args[index]);
        } else if (std.mem.eql(u8, arg, "--max-depth")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            max_depth = try std.fmt.parseInt(usize, args[index], 10);
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or source_name == null or target_name == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const toon = try graph_sqlite.shortestPathToon(
        db,
        allocator,
        source_name.?,
        target_name.?,
        if (edge_labels.items.len > 0) edge_labels.items else null,
        max_depth,
    );
    defer allocator.free(toon);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{s}\n", .{toon});
    try stdout.flush();
}

fn runGraphDiagnosticsCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var limit: usize = 200;
    var component_small_max: usize = 2;
    var format: []const u8 = "toon";

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--workspace-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            workspace_id = args[index];
        } else if (std.mem.eql(u8, arg, "--ontology-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            ontology_id = args[index];
        } else if (std.mem.eql(u8, arg, "--limit")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            limit = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--component-small-max")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            component_small_max = try std.fmt.parseInt(usize, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--format")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            format = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or workspace_id == null) return CliError.InvalidArguments;
    if (!std.mem.eql(u8, format, "json") and !std.mem.eql(u8, format, "toon")) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    var report = try graph_diagnostics.buildReport(db, allocator, .{
        .workspace_id = workspace_id.?,
        .ontology_id = ontology_id,
        .limit = limit,
        .component_small_max = component_small_max,
    });
    defer report.deinit(allocator);

    const payload = if (std.mem.eql(u8, format, "json"))
        try graph_diagnostics.reportJson(allocator, report)
    else
        try graph_diagnostics.reportToon(allocator, report);
    defer allocator.free(payload);
    try writeStdout("{s}\n", .{payload});
}

fn runGraphGapRulesImportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var input_path: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--input")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            input_path = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or input_path == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const json = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), input_path.?, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(json);

    const imported = try graph_diagnostics.importRulesJson(db, allocator, json);
    try writeStdout("{f}\n", .{std.json.fmt(.{ .ok = true, .imported = imported }, .{})});
}

fn runCoverageCommand(allocator: Allocator, args: []const []const u8, by_domain: bool) !void {
    var db_path: ?[]const u8 = null;
    var workspace_or_domain: ?[]const u8 = null;
    var entity_types = std.ArrayList([]const u8).empty;
    defer entity_types.deinit(allocator);

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (!by_domain and std.mem.eql(u8, arg, "--workspace-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            workspace_or_domain = args[index];
        } else if (by_domain and std.mem.eql(u8, arg, "--domain-or-workspace")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            workspace_or_domain = args[index];
        } else if (std.mem.eql(u8, arg, "--entity-type")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            try entity_types.append(allocator, args[index]);
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or workspace_or_domain == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    var resolved_workspace_id: ?[]const u8 = null;
    defer if (resolved_workspace_id) |value| allocator.free(value);

    const workspace_id = if (by_domain) blk: {
        resolved_workspace_id = try ontology_sqlite.resolveWorkspace(db, allocator, workspace_or_domain.?);
        if (resolved_workspace_id == null) return CliError.InvalidArguments;
        break :blk resolved_workspace_id.?;
    } else workspace_or_domain.?;

    const report = try ontology_sqlite.coverageReport(
        db,
        allocator,
        workspace_id,
        if (entity_types.items.len > 0) entity_types.items else null,
    );
    defer {
        allocator.free(report.summary.workspace_id);
        for (report.gaps) |gap| {
            allocator.free(gap.id);
            allocator.free(gap.label);
            allocator.free(gap.entity_type);
            allocator.free(gap.criticality);
        }
        allocator.free(report.gaps);
    }

    const toon = try toon_exports.encodeCoverageReportAlloc(allocator, report, toon_exports.default_options);
    defer allocator.free(toon);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{s}\n", .{toon});
    try stdout.flush();
}

fn runPackCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var user_id: ?[]const u8 = null;
    var query_text: ?[]const u8 = null;
    var scope: ?[]const u8 = null;
    var limit: usize = 15;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--user-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            user_id = args[index];
        } else if (std.mem.eql(u8, arg, "--query")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            query_text = args[index];
        } else if (std.mem.eql(u8, arg, "--scope")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            scope = args[index];
        } else if (std.mem.eql(u8, arg, "--limit")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            limit = try std.fmt.parseInt(usize, args[index], 10);
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or user_id == null or query_text == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const rows = try pragma_sqlite.packContextScoped(
        db,
        allocator,
        user_id.?,
        query_text.?,
        scope,
        limit,
    );
    defer {
        for (rows) |row| pragma_sqlite.deinitPackedRow(allocator, row);
        allocator.free(rows);
    }

    const toon = try toon_exports.encodePackContextAlloc(
        allocator,
        user_id.?,
        query_text.?,
        scope,
        rows,
        toon_exports.default_options,
    );
    defer allocator.free(toon);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{s}\n", .{toon});
    try stdout.flush();
}

fn runQueueSendCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var queue_name: ?[]const u8 = null;
    var message: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--queue")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            queue_name = args[index];
        } else if (std.mem.eql(u8, arg, "--message")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            message = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or queue_name == null or message == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const msg_id = try queueSend(db, allocator, queue_name.?, message.?);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(.{ .msg_id = msg_id }, .{})});
    try stdout.flush();
}

fn runQueueReadCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var queue_name: ?[]const u8 = null;
    var vt: i64 = 30;
    var limit: usize = 10;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--queue")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            queue_name = args[index];
        } else if (std.mem.eql(u8, arg, "--vt")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            vt = try std.fmt.parseInt(i64, args[index], 10);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            limit = try std.fmt.parseInt(usize, args[index], 10);
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or queue_name == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const rows = try queueRead(db, allocator, queue_name.?, vt, limit);
    defer {
        for (rows) |row| allocator.free(row.message);
        allocator.free(rows);
    }

    const payload = .{ .queue = queue_name.?, .messages = rows };
    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(payload, .{})});
    try stdout.flush();
}

fn runQueueArchiveCommand(allocator: Allocator, args: []const []const u8) !void {
    try runQueueMutationCommand(allocator, args, "archive");
}

fn runQueueDeleteCommand(allocator: Allocator, args: []const []const u8) !void {
    try runQueueMutationCommand(allocator, args, "delete");
}

fn runQueueMutationCommand(allocator: Allocator, args: []const []const u8, op: []const u8) !void {
    var db_path: ?[]const u8 = null;
    var queue_name: ?[]const u8 = null;
    var msg_id: ?i64 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--queue")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            queue_name = args[index];
        } else if (std.mem.eql(u8, arg, "--msg-id")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            msg_id = try std.fmt.parseInt(i64, args[index], 10);
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or queue_name == null or msg_id == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const ok = if (std.mem.eql(u8, op, "archive"))
        try queueArchive(db, allocator, queue_name.?, msg_id.?)
    else
        try queueDelete(db, allocator, queue_name.?, msg_id.?);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(.{ .ok = ok }, .{})});
    try stdout.flush();
}

const SimulationSummary = struct {
    queue_msg_id: i64,
    query_docs: usize,
    query_reachable_nodes: usize,
    query_projections: usize,
    workspace_tables: usize,
    pack_rows: usize,
    traverse_rows: usize,
    export_tables: usize,
};

/// Ensures standalone schema exists and loads the same demo graph/workspace data
/// as the in-memory `simulate` smoke path, but persists it to the given SQLite file.
/// Skips seeding if `graph_entity` already has rows (safe on dashboard restart).
fn runSeedDemoCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }
    if (db_path == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const c = facet_sqlite.c;
    const count_sql = "SELECT COUNT(*) FROM graph_entity";
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, count_sql.ptr, @intCast(count_sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return CliError.InvalidArguments;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) {
        return CliError.InvalidArguments;
    }
    const entity_count = c.sqlite3_column_int64(stmt.?, 0);

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;

    if (entity_count > 0) {
        try stdout.print("{f}\n", .{std.json.fmt(.{ .seeded = false, .skipped = true }, .{})});
        try stdout.flush();
        return;
    }

    try seedStandaloneSimulation(db, allocator);
    try stdout.print("{f}\n", .{std.json.fmt(.{ .seeded = true }, .{})});
    try stdout.flush();
}

fn runBootstrapFromSqlCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var sql_file_path: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            db_path = args[index];
        } else if (std.mem.eql(u8, arg, "--sql-file")) {
            index += 1;
            if (index >= args.len) return CliError.InvalidArguments;
            sql_file_path = args[index];
        } else {
            return CliError.InvalidArguments;
        }
    }

    if (db_path == null or sql_file_path == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const sql = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), sql_file_path.?, allocator, .unlimited);
    defer allocator.free(sql);
    try db.exec(sql);

    try graph_sqlite.rebuildAdjacency(db, allocator);
    try search_sqlite.rebuildSearchArtifacts(db, allocator);

    const payload = .{
        .graph_entities = try countRows(db, "graph_entity"),
        .graph_relations = try countRows(db, "graph_relation"),
        .facet_rows = try countRows(db, "facets"),
        .search_documents = try countRows(db, "search_documents"),
        .queues = try countRows(db, "queue_registry"),
    };

    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(payload, .{})});
    try stdout.flush();
}

fn runSimulationCommand(allocator: Allocator, args: []const []const u8) !void {
    if (args.len != 0) return CliError.InvalidArguments;
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();

    const summary = try runStandaloneSimulation(db, allocator);
    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print("{f}\n", .{std.json.fmt(summary, .{})});
    try stdout.flush();
}

fn countRows(db: facet_sqlite.Database, table_name: []const u8) !i64 {
    const c = facet_sqlite.c;
    const sql = try std.fmt.allocPrint(std.heap.page_allocator, "SELECT COUNT(*) FROM {s}", .{table_name});
    defer std.heap.page_allocator.free(sql);

    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null) != c.SQLITE_OK or stmt == null) {
        return CliError.InvalidArguments;
    }
    defer _ = c.sqlite3_finalize(stmt.?);
    if (c.sqlite3_step(stmt.?) != c.SQLITE_ROW) return CliError.InvalidArguments;
    return c.sqlite3_column_int64(stmt.?, 0);
}

pub fn runStandaloneSimulation(db: facet_sqlite.Database, allocator: Allocator) !SimulationSummary {
    try db.applyStandaloneSchema();
    try seedStandaloneSimulation(db, allocator);

    var queue = queue_sqlite.QueueStore.init(db, allocator);
    const queue_msg_id = try queue.sendText("simulation_firehose", "{\"event\":\"boot\"}");
    const queue_rows = try queue.read("simulation_firehose", 30, 10);
    defer {
        for (queue_rows) |row| allocator.free(row.message);
        allocator.free(queue_rows);
    }
    if (queue_rows.len > 0) {
        _ = try queue.archive("simulation_firehose", queue_rows[0].msg_id);
    }

    const workspace_toon = try workspace_sqlite.exportWorkspaceModelToon(db, allocator, "default");
    defer allocator.free(workspace_toon);

    const pack_rows = try pragma_sqlite.packContextScoped(db, allocator, "agent-q", "Ada", "player:7", 10);
    defer {
        for (pack_rows) |row| pragma_sqlite.deinitPackedRow(allocator, row);
        allocator.free(pack_rows);
    }

    const traverse_toon = try graph_sqlite.traverseToon(
        db,
        allocator,
        "Ada",
        .outbound,
        &.{"works_for"},
        3,
        "Acme",
    );
    defer allocator.free(traverse_toon);

    var runtime = try query_executor.Runtime.init(&db, allocator);
    defer runtime.deinit();

    var query_result = try runtime.execute(allocator, .{
        .workspace_id = "default",
        .table_name = "docs_fixture",
        .facet_filters = &.{
            .{ .facet_name = "category", .values = &.{"search"} },
            .{ .facet_name = "region", .values = &.{"eu"} },
        },
        .count_facets = &.{"category"},
        .graph = .{
            .start_nodes = &.{1},
            .max_hops = 2,
            .filter = .{ .edge_types = &.{"works_for"} },
        },
        .projection = .{
            .agent_id = "agent-q",
            .query = "Ada",
            .limit = 5,
        },
    });
    defer query_result.deinit(allocator);

    var export_model = try workspace_sqlite.exportWorkspaceModel(db, allocator, "default");
    defer workspace_sqlite.deinitWorkspaceExport(allocator, &export_model);

    return .{
        .queue_msg_id = queue_msg_id,
        .query_docs = if (query_result.documents) |bitmap| @intCast(bitmap.cardinality()) else 0,
        .query_reachable_nodes = if (query_result.reachable_nodes) |bitmap| @intCast(bitmap.cardinality()) else 0,
        .query_projections = query_result.projections.len,
        .workspace_tables = export_model.tables.len,
        .pack_rows = pack_rows.len,
        .traverse_rows = if (traverse_toon.len > 0) 1 else 0,
        .export_tables = export_model.tables.len,
    };
}

pub fn seedStandaloneSimulation(db: facet_sqlite.Database, allocator: Allocator) !void {
    try workspace_sqlite.upsertWorkspace(db, "default", "{\"domain\":\"navy\"}");
    try workspace_sqlite.upsertTableSemantic(db, 1, "default", "public", "docs_fixture", "id", "content", "metadata", "embedding", "english");
    try workspace_sqlite.upsertColumnSemantic(db, 1, 1, "id", "id", "uuid", false);
    try workspace_sqlite.upsertColumnSemantic(db, 2, 1, "content", "label", "text", true);
    try workspace_sqlite.upsertColumnSemantic(db, 3, 1, "metadata", "metadata", "json", true);
    try workspace_sqlite.upsertRelationSemantic(db, 1, "default", "documents_to_documents", 1, 1, "related_to", "{}");
    try workspace_sqlite.upsertSourceMapping(db, 1, "default", "vault:documents", "facet_projection_graph", 1, "{\"source\":\"vault\"}");

    try ontology_sqlite.importTaxonomyIntoFacets(db, allocator, 1, "public", "docs_fixture", 4, &.{
        .{
            .id = "facet-doc-1",
            .workspace_id = "default",
            .doc_id = 1,
            .node_id = "player:7",
            .label = "Ada works for Acme",
            .levels = &.{
                .{ .facet_id = 1, .facet_name = "category", .facet_value = "search" },
                .{ .facet_id = 2, .facet_name = "region", .facet_value = "eu" },
            },
        },
        .{
            .id = "facet-doc-3",
            .workspace_id = "default",
            .doc_id = 3,
            .node_id = "player:9",
            .label = "Graph doc",
            .levels = &.{
                .{ .facet_id = 1, .facet_name = "category", .facet_value = "search" },
                .{ .facet_id = 2, .facet_name = "region", .facet_value = "eu" },
            },
        },
    });

    _ = try ontology_sqlite.materializeTaxonomyProjections(db, allocator, "default", "agent-q");
    try ontology_sqlite.insertProjection(db, .{
        .id = "proj-1",
        .agent_id = "agent-q",
        .scope = "player:7",
        .proj_type = "canonical",
        .content = "Ada works for Acme",
        .weight = 1.0,
        .source_ref = "facet-doc-1",
        .source_type = "taxonomy",
        .status = "active",
    });
    try ontology_sqlite.insertProjection(db, .{
        .id = "proj-2",
        .agent_id = "agent-q",
        .scope = "default",
        .proj_type = "FACT",
        .content = "Ada works for Acme",
        .weight = 1.0,
        .source_ref = "facet-doc-1",
        .source_type = "taxonomy",
        .status = "active",
    });
    try pragma_sqlite.insertMemoryItem(db, .{
        .id = "mem-1",
        .user_id = "agent-q",
        .content = "Ada works for Acme",
    });
    try pragma_sqlite.insertMemoryProjection(db, .{
        .id = "mp-1",
        .item_id = "mem-1",
        .user_id = "agent-q",
        .projection_type = "canonical",
        .content = "Ada works for Acme",
        .rank_hint = 5,
        .confidence = 0.9,
        .metadata_json = "{\"scope\":\"player:7\"}",
        .facets_json = "{}",
    });

    try graph_sqlite.upsertEntity(db, 1, "person", "Ada");
    try graph_sqlite.upsertEntity(db, 2, "company", "Acme");
    try graph_sqlite.upsertRelation(db, .{
        .relation_id = 10,
        .source_id = 1,
        .target_id = 2,
        .relation_type = "works_for",
        .confidence = 0.9,
    });
    try graph_sqlite.upsertAdjacency(db, "graph_lj_out", 1, &.{10}, allocator);
    try graph_sqlite.upsertAdjacency(db, "graph_lj_in", 2, &.{10}, allocator);
    try graph_sqlite.insertEntityDocument(db, 1, 1, 1, "author", 0.95);
}

test "standalone simulation helper runs a representative smoke path" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();

    const summary = try runStandaloneSimulation(db, std.testing.allocator);
    try std.testing.expect(summary.queue_msg_id > 0);
    try std.testing.expect(summary.query_docs > 0);
    try std.testing.expect(summary.query_reachable_nodes > 0);
    try std.testing.expect(summary.query_projections > 0);
    try std.testing.expect(summary.workspace_tables > 0);
    try std.testing.expect(summary.pack_rows > 0);
    try std.testing.expect(summary.traverse_rows > 0);
    try std.testing.expect(summary.export_tables > 0);
}

test "coverage cli helper renders a report" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    try workspace_sqlite.upsertWorkspace(db, "default", "{\"domain\":\"navy\"}");
    try ontology_sqlite.upsertFacet(db, .{
        .id = "facet-1",
        .schema_id = "ghostcrab:taxonomy",
        .content = "Ada",
        .facets_json = "{\"node_id\":\"ada\",\"entity_type\":\"person\",\"label\":\"Ada\"}",
        .workspace_id = "default",
        .doc_id = 1,
    });
    try graph_sqlite.upsertEntityWithMetadata(db, 1, "person", "ada", "{\"workspace_id\":\"default\"}");

    const report = try ontology_sqlite.coverageReport(db, std.testing.allocator, "default", null);
    defer {
        std.testing.allocator.free(report.summary.workspace_id);
        for (report.gaps) |gap| {
            std.testing.allocator.free(gap.id);
            std.testing.allocator.free(gap.label);
            std.testing.allocator.free(gap.entity_type);
            std.testing.allocator.free(gap.criticality);
        }
        std.testing.allocator.free(report.gaps);
    }

    const toon = try toon_exports.encodeCoverageReportAlloc(std.testing.allocator, report, toon_exports.default_options);
    defer std.testing.allocator.free(toon);

    try std.testing.expect(std.mem.indexOf(u8, toon, "kind: coverage_report") != null);
    try std.testing.expect(std.mem.indexOf(u8, toon, "workspace_id: default") != null);
}

pub fn queueSend(
    db: facet_sqlite.Database,
    allocator: Allocator,
    queue_name: []const u8,
    message: []const u8,
) !i64 {
    var queue = queue_sqlite.QueueStore.init(db, allocator);
    return try queue.sendText(queue_name, message);
}

pub fn queueRead(
    db: facet_sqlite.Database,
    allocator: Allocator,
    queue_name: []const u8,
    vt: i64,
    limit: usize,
) ![]queue_sqlite.Message {
    var queue = queue_sqlite.QueueStore.init(db, allocator);
    return try queue.read(queue_name, vt, limit);
}

pub fn queueArchive(
    db: facet_sqlite.Database,
    allocator: Allocator,
    queue_name: []const u8,
    msg_id: i64,
) !bool {
    var queue = queue_sqlite.QueueStore.init(db, allocator);
    return try queue.archive(queue_name, msg_id);
}

pub fn queueDelete(
    db: facet_sqlite.Database,
    allocator: Allocator,
    queue_name: []const u8,
    msg_id: i64,
) !bool {
    var queue = queue_sqlite.QueueStore.init(db, allocator);
    return try queue.delete(queue_name, msg_id);
}

test "queue cli helpers use the sqlite queue store" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();

    const msg_id = try queueSend(db, std.testing.allocator, "signal_raw", "{\"hello\":\"world\"}");
    try std.testing.expect(msg_id > 0);

    const rows = try queueRead(db, std.testing.allocator, "signal_raw", 30, 10);
    defer {
        for (rows) |row| std.testing.allocator.free(row.message);
        std.testing.allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("{\"hello\":\"world\"}", rows[0].message);

    try std.testing.expect(try queueArchive(db, std.testing.allocator, "signal_raw", msg_id));

    const empty = try queueRead(db, std.testing.allocator, "signal_raw", 30, 10);
    defer {
        for (empty) |row| std.testing.allocator.free(row.message);
        std.testing.allocator.free(empty);
    }
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    const msg_id_2 = try queueSend(db, std.testing.allocator, "signal_raw", "{\"hello\":\"again\"}");
    try std.testing.expect(msg_id_2 > 0);
    try std.testing.expect(try queueDelete(db, std.testing.allocator, "signal_raw", msg_id_2));
}

// ---- Collections / workspace / ontology CLI verbs --------------------------

fn requireArg(args: []const []const u8, index: *usize) ![]const u8 {
    index.* += 1;
    if (index.* >= args.len) return CliError.InvalidArguments;
    return args[index.*];
}

fn runWorkspaceCreateCommand(allocator: Allocator, args: []const []const u8) !void {
    _ = allocator;
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var label: ?[]const u8 = null;
    var description: ?[]const u8 = null;
    var profile: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--label")) label = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--description")) description = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--profile")) profile = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{
        .workspace_id = workspace_id.?,
        .label = label,
        .description = description,
        .domain_profile = profile,
    });
    try writeStdout("workspace {s} ready\n", .{workspace_id.?});
}

fn runCollectionCreateCommand(allocator: Allocator, args: []const []const u8) !void {
    _ = allocator;
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var name: ?[]const u8 = null;
    var chunk_bits: u8 = 8;
    var language: []const u8 = "english";

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--name")) name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--chunk-bits")) {
            const v = try requireArg(args, &index);
            chunk_bits = std.fmt.parseInt(u8, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--language")) language = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or collection_id == null or name == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = workspace_id.?,
        .collection_id = collection_id.?,
        .name = name.?,
        .chunk_bits = chunk_bits,
        .default_language = language,
    });
    try writeStdout("collection {s} ready in workspace {s}\n", .{ collection_id.?, workspace_id.? });
}

fn runOntologyRegisterCommand(allocator: Allocator, args: []const []const u8) !void {
    _ = allocator;
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var name: ?[]const u8 = null;
    var version: []const u8 = "1.0.0";
    var source_kind: []const u8 = "constructed";

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--name")) name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--version")) version = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--source-kind")) source_kind = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or ontology_id == null or name == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = ontology_id.?,
        .workspace_id = workspace_id.?,
        .name = name.?,
        .version = version,
        .source_kind = source_kind,
    });
    try writeStdout("ontology {s} registered for workspace {s}\n", .{ ontology_id.?, workspace_id.? });
}

fn runOntologyAttachCommand(allocator: Allocator, args: []const []const u8) !void {
    _ = allocator;
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var role: []const u8 = "primary";

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--role")) role = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or collection_id == null or ontology_id == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.attachOntologyToCollection(db, workspace_id.?, collection_id.?, ontology_id.?, role);
    try writeStdout("ontology {s} attached to {s} ({s})\n", .{ ontology_id.?, collection_id.?, role });
}

fn runQualificationVocabListCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var taxonomy_filter: ?[]const u8 = null;
    var facet_filter: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--taxonomies")) taxonomy_filter = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--facets")) facet_filter = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"workspace_id\":");
    try writer.print("{f}", .{std.json.fmt(workspace_id.?, .{})});
    try writer.writeAll(",\"collection_id\":");
    if (collection_id) |collection| {
        try writer.print("{f}", .{std.json.fmt(collection, .{})});
    } else {
        try writer.writeAll("null");
    }
    try writer.writeAll(",\"taxonomies\":[");
    try writeQualificationTaxonomiesJson(writer, db, workspace_id.?, collection_id, taxonomy_filter);
    try writer.writeAll("],\"facets\":[");
    try writeQualificationFacetsJson(allocator, writer, db, workspace_id.?, collection_id, taxonomy_filter, facet_filter);
    try writer.writeAll("]}\n");

    const payload = try out.toOwnedSlice();
    defer allocator.free(payload);
    try writeStdout("{s}", .{payload});
}

fn writeQualificationTaxonomiesJson(
    writer: *std.Io.Writer,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
    taxonomy_filter: ?[]const u8,
) !void {
    const sql =
        \\SELECT o.ontology_id, o.name, o.version, o.source_kind, co.role
        \\FROM ontologies o
        \\LEFT JOIN collection_ontologies co
        \\  ON co.workspace_id = ?1
        \\ AND co.collection_id = ?2
        \\ AND co.ontology_id = o.ontology_id
        \\WHERE (o.workspace_id = ?1 OR o.workspace_id IS NULL OR co.ontology_id IS NOT NULL)
        \\  AND (?2 IS NULL OR co.collection_id = ?2)
        \\ORDER BY o.ontology_id
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_id) |collection| try facet_sqlite.bindText(stmt, 2, collection) else try facet_sqlite.bindNull(stmt, 2);

    const c = facet_sqlite.c;
    var first = true;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const ontology_id = columnText(stmt, 0);
        if (!csvContains(taxonomy_filter, ontology_id)) continue;

        if (!first) try writer.writeAll(",");
        first = false;
        try writer.writeAll("{\"id\":");
        try writer.print("{f}", .{std.json.fmt(ontology_id, .{})});
        try writer.writeAll(",\"name\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 1), .{})});
        try writer.writeAll(",\"version\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 2), .{})});
        try writer.writeAll(",\"source_kind\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 3), .{})});
        try writer.writeAll(",\"role\":");
        if (c.sqlite3_column_type(stmt, 4) == c.SQLITE_NULL) {
            try writer.writeAll("null");
        } else {
            try writer.print("{f}", .{std.json.fmt(columnText(stmt, 4), .{})});
        }
        try writer.writeAll("}");
    }
}

fn writeQualificationFacetsJson(
    allocator: Allocator,
    writer: *std.Io.Writer,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: ?[]const u8,
    taxonomy_filter: ?[]const u8,
    facet_filter: ?[]const u8,
) !void {
    const sql =
        \\SELECT d.ontology_id, d.namespace, d.dimension, d.value_type, d.is_multi, d.hierarchy_kind,
        \\       COUNT(v.value) AS values_count
        \\FROM ontology_dimensions d
        \\JOIN ontologies o ON o.ontology_id = d.ontology_id
        \\LEFT JOIN collection_ontologies co
        \\  ON co.workspace_id = ?1
        \\ AND co.collection_id = ?2
        \\ AND co.ontology_id = d.ontology_id
        \\LEFT JOIN ontology_values v
        \\  ON v.ontology_id = d.ontology_id
        \\ AND v.namespace = d.namespace
        \\ AND v.dimension = d.dimension
        \\WHERE (o.workspace_id = ?1 OR o.workspace_id IS NULL OR co.ontology_id IS NOT NULL)
        \\  AND (?2 IS NULL OR co.collection_id = ?2)
        \\GROUP BY d.ontology_id, d.namespace, d.dimension, d.value_type, d.is_multi, d.hierarchy_kind
        \\ORDER BY d.ontology_id, d.namespace, d.dimension
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    if (collection_id) |collection| try facet_sqlite.bindText(stmt, 2, collection) else try facet_sqlite.bindNull(stmt, 2);

    const c = facet_sqlite.c;
    var first = true;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;

        const ontology_id = columnText(stmt, 0);
        const namespace = columnText(stmt, 1);
        const dimension = columnText(stmt, 2);
        if (!csvContains(taxonomy_filter, ontology_id)) continue;
        if (!qualification_normalize.facetCsvContains(facet_filter, namespace, dimension)) continue;

        if (!first) try writer.writeAll(",");
        first = false;
        try writer.writeAll("{\"id\":");
        try writeFacetIdJson(allocator, writer, namespace, dimension);
        try writer.writeAll(",\"taxonomy_id\":");
        try writer.print("{f}", .{std.json.fmt(ontology_id, .{})});
        try writer.writeAll(",\"namespace\":");
        try writer.print("{f}", .{std.json.fmt(namespace, .{})});
        try writer.writeAll(",\"dimension\":");
        try writer.print("{f}", .{std.json.fmt(dimension, .{})});
        try writer.writeAll(",\"value_type\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 3), .{})});
        try writer.writeAll(",\"is_multi\":");
        try writer.writeAll(if (c.sqlite3_column_int64(stmt, 4) != 0) "true" else "false");
        try writer.writeAll(",\"hierarchy_kind\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 5), .{})});
        try writer.writeAll(",\"values_count\":");
        try writer.print("{}", .{c.sqlite3_column_int64(stmt, 6)});
        try writer.writeAll(",\"values_preview\":[");
        try writeFacetValuesPreviewJson(writer, db, ontology_id, namespace, dimension);
        try writer.writeAll("]}");
    }
}

fn writeFacetValuesPreviewJson(
    writer: *std.Io.Writer,
    db: facet_sqlite.Database,
    ontology_id: []const u8,
    namespace: []const u8,
    dimension: []const u8,
) !void {
    const sql =
        \\SELECT value
        \\FROM ontology_values
        \\WHERE ontology_id = ?1 AND namespace = ?2 AND dimension = ?3
        \\ORDER BY value_id
        \\LIMIT 8
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    try facet_sqlite.bindText(stmt, 2, namespace);
    try facet_sqlite.bindText(stmt, 3, dimension);

    const c = facet_sqlite.c;
    var first = true;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        if (!first) try writer.writeAll(",");
        first = false;
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 0), .{})});
    }
}

fn writeFacetIdJson(
    allocator: Allocator,
    writer: *std.Io.Writer,
    namespace: []const u8,
    dimension: []const u8,
) !void {
    const id = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ namespace, dimension });
    defer allocator.free(id);
    try writer.print("{f}", .{std.json.fmt(id, .{})});
}

fn columnText(stmt: *facet_sqlite.c.sqlite3_stmt, index: c_int) []const u8 {
    const len: usize = @intCast(facet_sqlite.c.sqlite3_column_bytes(stmt, index));
    const ptr = facet_sqlite.c.sqlite3_column_text(stmt, index) orelse return "";
    return @as([*]const u8, @ptrCast(ptr))[0..len];
}

fn csvContains(csv: ?[]const u8, value: []const u8) bool {
    const raw = csv orelse return true;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (std.mem.eql(u8, trimmed, value)) return true;
    }
    return false;
}

const QualificationAssignmentRow = qualification_apply.QualificationAssignmentRow;

const QualificationEnvelope = struct {
    assignments: []QualificationAssignmentRow = &.{},
};

fn runDocumentQualifyCommand(allocator: Allocator, args: []const []const u8, env_map: *const std.process.Environ.Map) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var taxonomy_filter: ?[]const u8 = null;
    var facet_filter: ?[]const u8 = null;
    var llm_provider_arg: ?[]const u8 = null;
    var base_url: ?[]const u8 = null;
    var api_key: ?[]const u8 = null;
    var model: ?[]const u8 = null;
    var mock_qualification_file: ?[]const u8 = null;
    var context_budget_json: ?[]const u8 = null;
    var dry_run = false;
    var limit: usize = 20;
    var target: []const u8 = "doc";
    var temperature: f32 = 0.0;
    var max_tokens: ?u32 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--taxonomies")) taxonomy_filter = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--facets")) facet_filter = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--llm-provider")) llm_provider_arg = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--base-url")) base_url = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--api-key")) api_key = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--model")) model = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--mock-qualification-json")) mock_qualification_file = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--dry-run")) dry_run = true else if (std.mem.eql(u8, arg, "--target")) target = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--limit")) {
            const v = try requireArg(args, &index);
            limit = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--temperature")) {
            const v = try requireArg(args, &index);
            temperature = std.fmt.parseFloat(f32, v) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            const v = try requireArg(args, &index);
            max_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--context-budget-json")) {
            context_budget_json = try requireArg(args, &index);
        } else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or collection_id == null or taxonomy_filter == null or facet_filter == null) return CliError.InvalidArguments;
    if (!std.mem.eql(u8, target, "doc") and !std.mem.eql(u8, target, "chunk") and !std.mem.eql(u8, target, "both")) return CliError.InvalidArguments;
    var loaded_env = try llm_provider.loadEnvValues(allocator, mindbrain.zig16_compat.io(), env_map);
    defer loaded_env.deinit(allocator);
    const resolved_provider = try llm_provider.resolve(.{
        .provider = llm_provider_arg,
        .base_url = base_url,
        .api_key = api_key,
        .model = model,
    }, loaded_env.values);

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const ontology_id = firstCsvValue(taxonomy_filter.?) orelse return CliError.InvalidArguments;
    const qualification_json = if (mock_qualification_file) |path|
        try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), path, allocator, .unlimited)
    else if (dry_run)
        try buildQualificationDryRunEnvelope(allocator, db, workspace_id.?, collection_id.?, ontology_id, facet_filter.?, target, limit)
    else
        try runLiveDocumentQualification(allocator, db, .{
            .workspace_id = workspace_id.?,
            .collection_id = collection_id.?,
            .ontology_id = ontology_id,
            .facet_filter = facet_filter.?,
            .target = target,
            .limit = limit,
            .provider = resolved_provider.provider,
            .temperature = temperature,
            .max_tokens = max_tokens,
            .context_budget_json = context_budget_json,
        });
    defer allocator.free(qualification_json);

    const applied = try qualification_apply.applyQualificationEnvelope(allocator, db, .{
        .workspace_id = workspace_id.?,
        .collection_id = collection_id.?,
        .ontology_id = ontology_id,
        .facet_filter = facet_filter.?,
        .source = if (mock_qualification_file != null) "mock-qualification-json" else if (dry_run) "dry-run" else "llm-document-qualify",
        .dry_run = dry_run,
        .json = qualification_json,
    });

    if (dry_run) {
        try writeStdout(
            "{{\"ok\":true,\"workspace_id\":{f},\"collection_id\":{f},\"dry_run\":true,\"accepted\":{},\"payload\":{s}}}\n",
            .{ std.json.fmt(workspace_id.?, .{}), std.json.fmt(collection_id.?, .{}), applied, qualification_json },
        );
    } else {
        try writeStdout("{f}\n", .{std.json.fmt(.{
            .ok = true,
            .workspace_id = workspace_id.?,
            .collection_id = collection_id.?,
            .dry_run = dry_run,
            .accepted = applied,
        }, .{})});
    }
}

fn firstCsvValue(csv: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len > 0) return trimmed;
    }
    return null;
}

const LiveQualificationOptions = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    facet_filter: []const u8,
    target: []const u8,
    limit: usize,
    provider: llm.ProviderConfig,
    temperature: f32,
    max_tokens: ?u32,
    context_budget_json: ?[]const u8 = null,
};

fn budgetProviderName(provider: llm.ProviderConfig) []const u8 {
    return switch (provider.kind) {
        .openai => "openai",
        else => provider.name,
    };
}

fn buildQualificationSystemPrompt(
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    facet_filter: []const u8,
    target: []const u8,
) ![]u8 {
    var system: std.Io.Writer.Allocating = .init(allocator);
    errdefer system.deinit();
    try system.writer.print(
        \\You qualify source documents against a controlled ontology.
        \\Return JSON exactly as {{"assignments":[...]}}.
        \\Each assignment requires target_kind, doc_id, namespace, dimension, value, weight.
        \\Optional field ontology_id (separate from namespace): use "{s}" when needed.
        \\namespace MUST be a facet namespace only: domain, source, or finance. DO NOT put ontology ids or "::" in namespace.
        \\dimension MUST be a facet name only: building, document_type, unit, etc. DO NOT put "namespace.dimension" in dimension.
        \\Example row: {{"target_kind":"doc","doc_id":1,"namespace":"domain","dimension":"building","value":"Résidence Les Tilleuls","weight":1}}
        \\Allowed target: {s}.
        \\Allowed facets (namespace.dimension): {s}.
        \\Use the taxonomy and facet vocabulary below when choosing namespace, dimension, and value.
        \\Only use values that are directly supported by the text. Do not invent facts.
        \\
        \\Vocabulary:
        \\
    , .{ ontology_id, target, facet_filter });
    try appendQualificationVocabulary(&system.writer, allocator, db, workspace_id, collection_id, ontology_id, facet_filter);
    return try system.toOwnedSlice();
}

fn callQualificationLlm(allocator: Allocator, opts: LiveQualificationOptions, prompt: PromptPair) ![]u8 {
    const messages = [_]llm.Message{
        .{ .role = "system", .content = prompt.system },
        .{ .role = "user", .content = prompt.user },
    };
    const manager = llm.Manager.init(.{
        .providers = &.{opts.provider},
        .default_provider = "default",
    });
    var response = manager.chat(allocator, mindbrain.zig16_compat.io(), &messages, .{
        .temperature = opts.temperature,
        .max_tokens = opts.max_tokens,
        .json_mode = true,
    }) catch |err| {
        if (err == error.HttpRequestFailed) {
            try writeLastLlmHttpFailure(allocator);
        }
        return err;
    };
    defer response.deinit(allocator);
    const content = std.mem.trim(u8, response.content, " \t\r\n");
    if (content.len == 0) {
        try writeLlmQualificationFailure(allocator, .{
            .reason = "empty_content",
            .model = opts.provider.model,
            .content = response.content,
            .raw_json = response.raw_json,
            .parse_error = null,
        });
        return error.InvalidLlmQualificationResponse;
    }
    validateQualificationEnvelope(allocator, content) catch |err| {
        try writeLlmQualificationFailure(allocator, .{
            .reason = "invalid_json",
            .model = opts.provider.model,
            .content = response.content,
            .raw_json = response.raw_json,
            .parse_error = @errorName(err),
        });
        return error.InvalidLlmQualificationResponse;
    };
    return try allocator.dupe(u8, content);
}

fn runLiveDocumentQualification(allocator: Allocator, db: facet_sqlite.Database, opts: LiveQualificationOptions) ![]u8 {
    const budget_ctx = PromptBudgetContext{
        .provider = budgetProviderName(opts.provider),
        .model = opts.provider.model,
        .context_budget_json = opts.context_budget_json,
    };
    const user_prefix =
        \\Classify the following MindBrain documents.
        \\
        \\Documents:
        \\
    ;

    var docs = try loadPromptDocuments(allocator, db, opts.workspace_id, opts.collection_id, opts.limit, null);
    defer docs.deinit(allocator);
    const system = try buildQualificationSystemPrompt(allocator, db, opts.workspace_id, opts.collection_id, opts.ontology_id, opts.facet_filter, opts.target);
    defer allocator.free(system);

    const spec = try llm_context_budget.loadBudget(allocator, budget_ctx.provider, budget_ctx.model, budget_ctx.context_budget_json);
    var doc_ids = try allocator.alloc(u64, docs.rows.len);
    defer allocator.free(doc_ids);
    var content_lengths = try allocator.alloc(usize, docs.rows.len);
    defer allocator.free(content_lengths);
    var metadata_total: usize = 0;
    for (docs.rows, 0..) |row, i| {
        doc_ids[i] = row.doc_id;
        content_lengths[i] = row.content.len;
        metadata_total += estimateQualificationDocMetadataChars(row);
    }
    var budget_report = try llm_context_budget.computeDocumentCharLimits(
        allocator,
        spec,
        doc_ids,
        content_lengths,
        system.len,
        user_prefix.len + metadata_total,
    );
    budget_report.provider = budget_ctx.provider;
    budget_report.model = budget_ctx.model;
    defer budget_report.deinit(allocator);
    try llm_context_budget.writeContextBudgetReport(allocator, budget_report);

    if (budget_report.needs_batch) {
        const batches = try llm_context_budget.planBudgetBatches(allocator, spec, doc_ids, content_lengths, system.len, user_prefix.len + metadata_total);
        defer {
            for (batches) |batch| allocator.free(batch);
            allocator.free(batches);
        }
        try llm_context_budget.writeContextBudgetBatchReport(allocator, batches);

        var parts = std.ArrayList([]u8).empty;
        errdefer {
            for (parts.items) |part| allocator.free(part);
            parts.deinit(allocator);
        }
        for (batches) |batch| {
            var prompt = try buildQualificationPrompts(allocator, db, opts.workspace_id, opts.collection_id, opts.ontology_id, opts.facet_filter, opts.target, opts.limit, .{
                .provider = budget_ctx.provider,
                .model = budget_ctx.model,
                .context_budget_json = budget_ctx.context_budget_json,
                .doc_ids = batch,
                .write_budget_report = false,
            });
            defer prompt.deinit(allocator);
            const part = try callQualificationLlm(allocator, opts, prompt);
            try parts.append(allocator, part);
        }
        return try mergeQualificationEnvelopes(allocator, parts.items);
    }

    var prompt = try buildQualificationPrompts(allocator, db, opts.workspace_id, opts.collection_id, opts.ontology_id, opts.facet_filter, opts.target, opts.limit, .{
        .provider = budget_ctx.provider,
        .model = budget_ctx.model,
        .context_budget_json = budget_ctx.context_budget_json,
        .write_budget_report = false,
    });
    defer prompt.deinit(allocator);
    return try callQualificationLlm(allocator, opts, prompt);
}

fn writeLastLlmHttpFailure(allocator: Allocator) !void {
    const json = try llm.http_client.lastHttpFailureJson(allocator) orelse return;
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("LLM_HTTP_FAILURE_JSON={s}\n", .{json});
    try stderr.flush();
}

const LlmQualificationFailure = struct {
    reason: []const u8,
    model: []const u8,
    content: []const u8,
    raw_json: []const u8,
    parse_error: ?[]const u8 = null,
};

fn writeLlmQualificationFailure(allocator: Allocator, failure: LlmQualificationFailure) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, failure, .{});
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("LLM_QUALIFICATION_FAILURE_JSON={s}\n", .{json});
    try stderr.flush();
}

fn validateQualificationEnvelope(allocator: Allocator, json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(QualificationEnvelope, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
}

fn buildQualificationDryRunEnvelope(
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    facet_filter: []const u8,
    target: []const u8,
    limit: usize,
) ![]u8 {
    var prompt = try buildQualificationPrompts(allocator, db, workspace_id, collection_id, ontology_id, facet_filter, target, limit, .{});
    defer prompt.deinit(allocator);
    return try std.json.Stringify.valueAlloc(allocator, .{
        .assignments = &[_]QualificationAssignmentRow{},
        .dry_run_system_prompt = prompt.system,
        .dry_run_user_prompt = prompt.user,
    }, .{});
}

const PromptPair = struct {
    system: []u8,
    user: []u8,

    fn deinit(self: PromptPair, allocator: Allocator) void {
        allocator.free(self.system);
        allocator.free(self.user);
    }
};

const PromptBudgetContext = struct {
    provider: []const u8 = "openai",
    model: []const u8 = "gpt-5-mini",
    context_budget_json: ?[]const u8 = null,
    doc_ids: ?[]const u64 = null,
    write_budget_report: bool = true,
};

const PromptDocumentRow = struct {
    doc_id: u64,
    source_ref: []const u8,
    summary: []const u8,
    content: []const u8,
};

const PromptDocumentList = struct {
    rows: []PromptDocumentRow,

    fn deinit(self: PromptDocumentList, allocator: Allocator) void {
        for (self.rows) |row| {
            allocator.free(row.source_ref);
            allocator.free(row.summary);
            allocator.free(row.content);
        }
        allocator.free(self.rows);
    }
};

fn dupeOptionalColumnText(allocator: Allocator, stmt: *facet_sqlite.c.sqlite3_stmt, index: c_int) ![]const u8 {
    if (facet_sqlite.c.sqlite3_column_type(stmt, index) == facet_sqlite.c.SQLITE_NULL) {
        return try allocator.dupe(u8, "");
    }
    return try facet_sqlite.dupeColumnText(allocator, stmt, index);
}

fn loadPromptDocuments(
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    limit: usize,
    doc_ids: ?[]const u64,
) !PromptDocumentList {
    const sql =
        \\SELECT doc_id, source_ref, summary, content
        \\FROM documents_raw
        \\WHERE workspace_id = ?1 AND collection_id = ?2
        \\ORDER BY doc_id
        \\LIMIT ?3
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);
    try facet_sqlite.bindText(stmt, 2, collection_id);
    try facet_sqlite.bindInt64(stmt, 3, @as(i64, @intCast(limit)));

    var rows = std.ArrayList(PromptDocumentRow).empty;
    errdefer {
        for (rows.items) |row| {
            allocator.free(row.source_ref);
            allocator.free(row.summary);
            allocator.free(row.content);
        }
        rows.deinit(allocator);
    }

    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const doc_id = @as(u64, @intCast(c.sqlite3_column_int64(stmt, 0)));
        if (doc_ids) |filter| {
            var include = false;
            for (filter) |wanted| {
                if (wanted == doc_id) {
                    include = true;
                    break;
                }
            }
            if (!include) continue;
        }
        const source_ref = try dupeOptionalColumnText(allocator, stmt, 1);
        errdefer allocator.free(source_ref);
        const summary = try dupeOptionalColumnText(allocator, stmt, 2);
        errdefer allocator.free(summary);
        const content = try facet_sqlite.dupeColumnText(allocator, stmt, 3);
        try rows.append(allocator, .{
            .doc_id = doc_id,
            .source_ref = source_ref,
            .summary = summary,
            .content = content,
        });
    }

    return .{
        .rows = try rows.toOwnedSlice(allocator),
    };
}

fn estimateQualificationDocMetadataChars(row: PromptDocumentRow) usize {
    return std.fmt.count("doc_id={d}\nsource_ref={s}\nsummary={s}\ncontent:\n\n---\n", .{
        row.doc_id,
        row.source_ref,
        row.summary,
    });
}

fn estimateBusinessExtractDocMetadataChars(row: PromptDocumentRow) usize {
    return std.fmt.count("{{\"doc_id\":{},\"source_ref\":{f},\"content\":{f}}}", .{
        row.doc_id,
        std.json.fmt(row.source_ref, .{}),
        std.json.fmt("", .{}),
    });
}

fn appendQualificationDocumentsWithLimits(
    writer: *std.Io.Writer,
    docs: []const PromptDocumentRow,
    limits: []const usize,
) !void {
    std.debug.assert(docs.len == limits.len);
    for (docs, limits) |row, limit| {
        const content = llm_context_budget.limitedContent(row.content, limit);
        try writer.print("doc_id={d}\nsource_ref={s}\nsummary={s}\ncontent:\n{s}\n---\n", .{
            row.doc_id,
            row.source_ref,
            row.summary,
            content,
        });
    }
}

fn appendBusinessExtractionDocumentsJsonWithLimits(
    writer: *std.Io.Writer,
    docs: []const PromptDocumentRow,
    limits: []const usize,
) !void {
    std.debug.assert(docs.len == limits.len);
    try writer.writeAll("[");
    var first = true;
    for (docs, limits) |row, limit| {
        const content = llm_context_budget.limitedContent(row.content, limit);
        if (!first) try writer.writeAll(",");
        first = false;
        try writer.writeAll("{\"doc_id\":");
        try writer.print("{}", .{row.doc_id});
        try writer.writeAll(",\"source_ref\":");
        try writer.print("{f}", .{std.json.fmt(row.source_ref, .{})});
        try writer.writeAll(",\"content\":");
        try writer.print("{f}", .{std.json.fmt(content, .{})});
        try writer.writeAll("}");
    }
    try writer.writeAll("]");
}

fn computePromptDocumentLimits(
    allocator: Allocator,
    budget_ctx: PromptBudgetContext,
    docs: []const PromptDocumentRow,
    system_chars: usize,
    fixed_user_chars: usize,
    metadata_chars_fn: *const fn (PromptDocumentRow) usize,
) !struct { limits: []usize, report: llm_context_budget.PromptBudgetReport } {
    const spec = try llm_context_budget.loadBudget(allocator, budget_ctx.provider, budget_ctx.model, budget_ctx.context_budget_json);
    var doc_ids = try allocator.alloc(u64, docs.len);
    errdefer allocator.free(doc_ids);
    var content_lengths = try allocator.alloc(usize, docs.len);
    errdefer allocator.free(content_lengths);
    var metadata_total: usize = 0;
    for (docs, 0..) |row, i| {
        doc_ids[i] = row.doc_id;
        content_lengths[i] = row.content.len;
        metadata_total += metadata_chars_fn(row);
    }
    var report = try llm_context_budget.computeDocumentCharLimits(
        allocator,
        spec,
        doc_ids,
        content_lengths,
        system_chars,
        fixed_user_chars + metadata_total,
    );
    allocator.free(doc_ids);
    allocator.free(content_lengths);
    report.provider = budget_ctx.provider;
    report.model = budget_ctx.model;
    return .{ .limits = report.per_doc_limits, .report = report };
}

fn mergeQualificationEnvelopes(allocator: Allocator, parts: []const []const u8) ![]u8 {
    var merged = std.ArrayList(QualificationAssignmentRow).empty;
    errdefer merged.deinit(allocator);
    for (parts) |json| {
        var parsed = try std.json.parseFromSlice(QualificationEnvelope, allocator, json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        try merged.appendSlice(allocator, parsed.value.assignments);
    }
    return try std.json.Stringify.valueAlloc(allocator, .{ .assignments = merged.items }, .{});
}

fn mergeBusinessExtractionEnvelopes(allocator: Allocator, parts: []const []const u8) ![]u8 {
    var parsed_parts = std.ArrayList(std.json.Parsed(BusinessExtractionEnvelope)).empty;
    defer {
        for (parsed_parts.items) |*parsed| parsed.deinit();
        parsed_parts.deinit(allocator);
    }
    for (parts, 0..) |json, batch_index| {
        const parse_opts = std.json.ParseOptions{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        };
        const parsed = parseBusinessExtractionEnvelopeJson(allocator, json, parse_opts) catch |err| {
            const detail = try std.fmt.allocPrint(allocator, "merge_parse_failed err={s} json_chars={d}", .{
                @errorName(err),
                json.len,
            });
            defer allocator.free(detail);
            try writeBusinessExtractProgress("merge_parse_failed", batch_index, parts.len, 0, "", 0, detail);
            return err;
        };
        try parsed_parts.append(allocator, parsed);
    }

    var entities = std.StringHashMap(BusinessEntityRow).init(allocator);
    defer entities.deinit();
    var relations = std.StringHashMap(BusinessRelationRow).init(allocator);
    defer relations.deinit();
    var aliases = std.ArrayList(BusinessAliasRow).empty;
    defer aliases.deinit(allocator);
    var entity_documents = std.ArrayList(BusinessEntityDocumentRow).empty;
    defer entity_documents.deinit(allocator);
    var entity_chunks = std.ArrayList(BusinessEntityChunkRow).empty;
    defer entity_chunks.deinit(allocator);
    var relation_properties = std.ArrayList(BusinessRelationPropertyRow).empty;
    defer relation_properties.deinit(allocator);

    for (parsed_parts.items) |parsed| {
        for (parsed.value.entities_raw) |row| {
            try entities.put(row.external_id, row);
        }
        for (parsed.value.relations_raw) |row| {
            try relations.put(row.external_id, row);
        }
        try aliases.appendSlice(allocator, parsed.value.entity_aliases_raw);
        try entity_documents.appendSlice(allocator, parsed.value.entity_documents_raw);
        try entity_chunks.appendSlice(allocator, parsed.value.entity_chunks_raw);
        try relation_properties.appendSlice(allocator, parsed.value.relation_properties_raw);
    }

    var entity_values = try allocator.alloc(BusinessEntityRow, entities.count());
    defer allocator.free(entity_values);
    var entity_index: usize = 0;
    var entity_iterator = entities.iterator();
    while (entity_iterator.next()) |entry| : (entity_index += 1) {
        entity_values[entity_index] = entry.value_ptr.*;
    }

    var relation_values = try allocator.alloc(BusinessRelationRow, relations.count());
    defer allocator.free(relation_values);
    var relation_index: usize = 0;
    var relation_iterator = relations.iterator();
    while (relation_iterator.next()) |entry| : (relation_index += 1) {
        relation_values[relation_index] = entry.value_ptr.*;
    }

    return try std.json.Stringify.valueAlloc(allocator, .{
        .entities_raw = entity_values,
        .relations_raw = relation_values,
        .entity_aliases_raw = aliases.items,
        .entity_documents_raw = entity_documents.items,
        .entity_chunks_raw = entity_chunks.items,
        .relation_properties_raw = relation_properties.items,
    }, .{});
}

fn writeCoverageGapReport(
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    expected_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, expected_json, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const counts_val = parsed.value.object.get("counts") orelse return;
    if (counts_val != .object) return;
    const counts = counts_val.object;

    const stmt = try facet_sqlite.prepare(
        db,
        "SELECT entity_type, COUNT(*) FROM entities_raw WHERE workspace_id = ?1 GROUP BY entity_type",
    );
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, workspace_id);

    var actual = std.StringHashMap(usize).init(allocator);
    defer {
        var it = actual.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        actual.deinit();
    }
    const c = facet_sqlite.c;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        const ptr = c.sqlite3_column_text(stmt, 0) orelse continue;
        const entity_type = ptr[0..@intCast(c.sqlite3_column_bytes(stmt, 0))];
        const count = @as(usize, @intCast(c.sqlite3_column_int64(stmt, 1)));
        try actual.put(try allocator.dupe(u8, entity_type), count);
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    try out.writer.writeAll("{");
    var first = true;
    var iterator = counts.iterator();
    while (iterator.next()) |entry| {
        const expected_count: usize = switch (entry.value_ptr.*) {
            .integer => |v| @intCast(v),
            .float => |v| @intCast(@as(i64, @intFromFloat(v))),
            else => continue,
        };
        const mapped_type = expectedKeyToEntityType(entry.key_ptr.*);
        const actual_count = actual.get(mapped_type) orelse actual.get(entry.key_ptr.*) orelse 0;
        if (actual_count >= expected_count) continue;
        if (!first) try out.writer.writeAll(",");
        first = false;
        try out.writer.print(
            "{f}:{{\"expected\":{},\"actual\":{}}}",
            .{ std.json.fmt(entry.key_ptr.*, .{}), expected_count, actual_count },
        );
    }
    if (first) return;
    try out.writer.writeAll("}");

    const json = try out.toOwnedSlice();
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("COVERAGE_GAP_JSON={s}\n", .{json});
    try stderr.flush();
}

fn writeBusinessExtractProgress(
    step: []const u8,
    batch_index: usize,
    batch_total: usize,
    doc_id: u64,
    source_ref: []const u8,
    content_chars: usize,
    detail: []const u8,
) !void {
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print(
        "BUSINESS_EXTRACT_PROGRESS step={s} batch={d}/{d} doc_id={d} source_ref={s} chars={d} {s}\n",
        .{ step, batch_index + 1, batch_total, doc_id, source_ref, content_chars, detail },
    );
    try stderr.flush();
}

fn findPromptDocumentRow(rows: []const PromptDocumentRow, doc_id: u64) ?PromptDocumentRow {
    for (rows) |row| {
        if (row.doc_id == doc_id) return row;
    }
    return null;
}

fn logBusinessExtractBatchDocs(
    step: []const u8,
    batch_index: usize,
    batch_total: usize,
    batch: []const u64,
    all_docs: []const PromptDocumentRow,
) !void {
    for (batch) |doc_id| {
        if (findPromptDocumentRow(all_docs, doc_id)) |row| {
            try writeBusinessExtractProgress(step, batch_index, batch_total, doc_id, row.source_ref, row.content.len, "");
        } else {
            try writeBusinessExtractProgress(step, batch_index, batch_total, doc_id, "?", 0, "doc_not_loaded");
        }
    }
}

fn summarizeBusinessExtractionEnvelope(json: []const u8) struct { entities: usize, relations: usize, entity_documents: usize } {
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, json, .{
        .ignore_unknown_fields = true,
    }) catch return .{ .entities = 0, .relations = 0, .entity_documents = 0 };
    defer parsed.deinit();
    if (parsed.value != .object) return .{ .entities = 0, .relations = 0, .entity_documents = 0 };
    const obj = parsed.value.object;
    return .{
        .entities = arrayLen(obj.get("entities_raw")),
        .relations = arrayLen(obj.get("relations_raw")),
        .entity_documents = arrayLen(obj.get("entity_documents_raw")),
    };
}

fn arrayLen(value: ?std.json.Value) usize {
    const v = value orelse return 0;
    return switch (v) {
        .array => |arr| arr.items.len,
        else => 0,
    };
}

fn expectedKeyToEntityType(key: []const u8) []const u8 {
    if (std.mem.eql(u8, key, "buildings")) return "building";
    if (std.mem.eql(u8, key, "blocks")) return "block";
    if (std.mem.eql(u8, key, "units")) return "unit";
    if (std.mem.eql(u8, key, "households")) return "household";
    if (std.mem.eql(u8, key, "cellars")) return "cellar";
    if (std.mem.eql(u8, key, "private_gardens")) return "private_garden";
    if (std.mem.eql(u8, key, "lease_contracts")) return "lease_contract";
    if (std.mem.eql(u8, key, "coda_entries")) return "coda_entry";
    return key;
}

fn buildQualificationPrompts(
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    facet_filter: []const u8,
    target: []const u8,
    limit: usize,
    budget_ctx: PromptBudgetContext,
) !PromptPair {
    const system = try buildQualificationSystemPrompt(allocator, db, workspace_id, collection_id, ontology_id, facet_filter, target);
    errdefer allocator.free(system);

    const user_prefix =
        \\Classify the following MindBrain documents.
        \\
        \\Documents:
        \\
    ;
    var docs = try loadPromptDocuments(allocator, db, workspace_id, collection_id, limit, budget_ctx.doc_ids);
    defer docs.deinit(allocator);

    var budget = try computePromptDocumentLimits(
        allocator,
        budget_ctx,
        docs.rows,
        system.len,
        user_prefix.len,
        estimateQualificationDocMetadataChars,
    );
    defer budget.report.deinit(allocator);
    if (budget_ctx.write_budget_report and budget_ctx.doc_ids == null) {
        try llm_context_budget.writeContextBudgetReport(allocator, budget.report);
    }

    var user: std.Io.Writer.Allocating = .init(allocator);
    errdefer user.deinit();
    try user.writer.writeAll(user_prefix);
    try appendQualificationDocumentsWithLimits(&user.writer, docs.rows, budget.limits);
    return .{
        .system = system,
        .user = try user.toOwnedSlice(),
    };
}

fn seedPromptSplitFixture(db: facet_sqlite.Database) !void {
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-prompt" });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws-prompt",
        .collection_id = "ws-prompt::docs",
        .name = "docs",
    });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-prompt::core",
        .workspace_id = "ws-prompt",
        .name = "core",
    });
    try collections_sqlite.attachOntologyToCollection(db, "ws-prompt", "ws-prompt::docs", "ws-prompt::core", "primary");
    try collections_sqlite.ensureDimension(db, .{
        .ontology_id = "ws-prompt::core",
        .namespace = "topic",
        .dimension = "category",
    });
    try collections_sqlite.ensureValue(db, .{
        .ontology_id = "ws-prompt::core",
        .namespace = "topic",
        .dimension = "category",
        .value_id = 1,
        .value = "access-control",
    });
    try collections_sqlite.ensureEntityType(db, .{
        .ontology_id = "ws-prompt::core",
        .entity_type = "policy",
        .label = "Policy",
    });
    try collections_sqlite.ensureEdgeType(db, .{
        .ontology_id = "ws-prompt::core",
        .edge_type = "GOVERNS",
        .source_entity_type = "policy",
        .target_entity_type = "policy",
    });
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws-prompt",
        .collection_id = "ws-prompt::docs",
        .doc_id = 1,
        .source_ref = "policy.md",
        .summary = "Access decisions must be documented.",
        .content = "Article 1. Operators must document access control decisions.",
    });
}

test "qualification prompt keeps ontology vocabulary in system and documents in user" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedPromptSplitFixture(db);

    var prompt = try buildQualificationPrompts(std.testing.allocator, db, "ws-prompt", "ws-prompt::docs", "ws-prompt::core", "topic.category", "doc", 1, .{});
    defer prompt.deinit(std.testing.allocator);

    try std.testing.expect(std.mem.indexOf(u8, prompt.system, "\"taxonomies\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.system, "access-control") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.system, "DO NOT put ontology ids") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "Article 1. Operators") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "\"taxonomies\"") == null);
}

fn appendQualificationVocabulary(
    writer: *std.Io.Writer,
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    facet_filter: []const u8,
) !void {
    try writer.writeAll("{\"taxonomies\":[");
    try writeQualificationTaxonomiesJson(writer, db, workspace_id, collection_id, ontology_id);
    try writer.writeAll("],\"facets\":[");
    try writeQualificationFacetsJson(allocator, writer, db, workspace_id, collection_id, ontology_id, if (facet_filter.len == 0) null else facet_filter);
    try writer.writeAll("],\"entity_types\":[");
    try writeQualificationEntityTypesJson(writer, db, ontology_id);
    try writer.writeAll("],\"edge_types\":[");
    try writeQualificationEdgeTypesJson(writer, db, ontology_id);
    try writer.writeAll("]}");
}

fn writeQualificationEntityTypesJson(
    writer: *std.Io.Writer,
    db: facet_sqlite.Database,
    ontology_id: []const u8,
) !void {
    const sql =
        \\SELECT entity_type, label
        \\FROM ontology_entity_types
        \\WHERE ontology_id = ?1
        \\ORDER BY entity_type
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);

    const c = facet_sqlite.c;
    var first = true;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        if (!first) try writer.writeAll(",");
        first = false;
        try writer.writeAll("{\"entity_type\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 0), .{})});
        try writer.writeAll(",\"label\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 1), .{})});
        try writer.writeAll("}");
    }
}

fn writeQualificationEdgeTypesJson(
    writer: *std.Io.Writer,
    db: facet_sqlite.Database,
    ontology_id: []const u8,
) !void {
    const sql =
        \\SELECT edge_type, source_entity_type, target_entity_type
        \\FROM ontology_edge_types
        \\WHERE ontology_id = ?1
        \\ORDER BY edge_type
    ;
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);

    const c = facet_sqlite.c;
    var first = true;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return error.StepFailed;
        if (!first) try writer.writeAll(",");
        first = false;
        try writer.writeAll("{\"edge_type\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 0), .{})});
        try writer.writeAll(",\"source_entity_type\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 1), .{})});
        try writer.writeAll(",\"target_entity_type\":");
        try writer.print("{f}", .{std.json.fmt(columnText(stmt, 2), .{})});
        try writer.writeAll("}");
    }
}


fn runDocumentBusinessExtractCommand(allocator: Allocator, args: []const []const u8, env_map: *const std.process.Environ.Map) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var expected_path: ?[]const u8 = null;
    var llm_provider_arg: ?[]const u8 = null;
    var base_url: ?[]const u8 = null;
    var api_key: ?[]const u8 = null;
    var model: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var raw_output_path: ?[]const u8 = null;
    var request_output_path: ?[]const u8 = null;
    var input_json_path: ?[]const u8 = null;
    var reindex_mode: BusinessReindexMode = .none;
    var limit: usize = 20;
    var batch_size: usize = 0;
    var llm_parallel: usize = 1;
    var context_budget_json: ?[]const u8 = null;
    var temperature: f32 = 0.0;
    var max_tokens: ?u32 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--expected-coverage-json")) expected_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--llm-provider")) llm_provider_arg = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--base-url")) base_url = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--api-key")) api_key = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--model")) model = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output")) output_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--raw-output")) raw_output_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--request-output")) request_output_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--input-json")) input_json_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--reindex")) {
            const mode = try requireArg(args, &index);
            if (std.mem.eql(u8, mode, "none")) reindex_mode = .none else if (std.mem.eql(u8, mode, "graph")) reindex_mode = .graph else return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--limit")) {
            const v = try requireArg(args, &index);
            limit = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            const v = try requireArg(args, &index);
            batch_size = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--llm-parallel")) {
            const v = try requireArg(args, &index);
            llm_parallel = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
            if (llm_parallel == 0) return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--context-budget-json")) {
            context_budget_json = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--temperature")) {
            const v = try requireArg(args, &index);
            temperature = std.fmt.parseFloat(f32, v) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            const v = try requireArg(args, &index);
            max_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or collection_id == null or ontology_id == null) return CliError.InvalidArguments;
    if (input_json_path == null and expected_path == null) return CliError.InvalidArguments;
    var loaded_env = try llm_provider.loadEnvValues(allocator, mindbrain.zig16_compat.io(), env_map);
    defer loaded_env.deinit(allocator);
    const resolved_provider = try llm_provider.resolve(.{
        .provider = llm_provider_arg,
        .base_url = base_url,
        .api_key = api_key,
        .model = model,
    }, loaded_env.values);

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    if (input_json_path) |path| {
        const content = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), path, allocator, .limited(64 * 1024 * 1024));
        defer allocator.free(content);
        const trimmed = std.mem.trim(u8, content, " \t\r\n");
        const expected_for_coverage = if (expected_path) |coverage_path|
            try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), coverage_path, allocator, .limited(16 * 1024 * 1024))
        else
            null;
        defer if (expected_for_coverage) |json| allocator.free(json);
        try applyAndWriteBusinessExtraction(allocator, db, .{
            .workspace_id = workspace_id.?,
            .collection_id = collection_id.?,
            .ontology_id = ontology_id.?,
            .json = trimmed,
            .expected_json = expected_for_coverage,
        }, output_path, reindex_mode);
        return;
    }

    const expected_json = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), expected_path.?, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(expected_json);

    try writeBusinessExtractProgress("command_start", 0, 1, 0, "", 0, "live_extract");

    const content = try runLiveBusinessExtraction(allocator, db, .{
        .workspace_id = workspace_id.?,
        .collection_id = collection_id.?,
        .ontology_id = ontology_id.?,
        .expected_json = expected_json,
        .limit = limit,
        .batch_size = batch_size,
        .llm_parallel = llm_parallel,
        .provider = resolved_provider.provider,
        .temperature = temperature,
        .max_tokens = max_tokens,
        .context_budget_json = context_budget_json,
        .request_output_path = request_output_path,
        .raw_output_path = raw_output_path,
    });
    defer allocator.free(content);

    try writeBusinessExtractProgress("apply_start", 0, 1, 0, "", 0, "db_apply");
    try applyAndWriteBusinessExtraction(allocator, db, .{
        .workspace_id = workspace_id.?,
        .collection_id = collection_id.?,
        .ontology_id = ontology_id.?,
        .json = content,
        .expected_json = expected_json,
    }, output_path, reindex_mode);
    try writeBusinessExtractProgress("command_done", 0, 1, 0, "", 0, "ok");
}

fn applyAndWriteBusinessExtraction(
    allocator: Allocator,
    db: facet_sqlite.Database,
    opts: BusinessExtractionApplyOptions,
    output_path: ?[]const u8,
    reindex_mode: BusinessReindexMode,
) !void {
    _ = try applyBusinessExtractionEnvelope(allocator, db, opts);
    try writeBusinessExtractProgress("apply_done", 0, 1, 0, "", 0, "entities_committed");
    if (reindex_mode == .graph) {
        try writeBusinessExtractProgress("reindex_start", 0, 1, 0, "", 0, "graph");
        _ = try reindex_http.reindexGraph(allocator, &db, opts.workspace_id, null);
        try writeBusinessExtractProgress("reindex_done", 0, 1, 0, "", 0, "graph");
    }
    if (opts.expected_json) |expected_json| {
        try writeCoverageGapReport(allocator, db, opts.workspace_id, expected_json);
    }
    if (output_path) |path| {
        try std.Io.Dir.cwd().writeFile(mindbrain.zig16_compat.io(), .{
            .sub_path = path,
            .data = opts.json,
            .flags = .{ .truncate = true },
        });
    } else {
        try writeStdout("{s}\n", .{opts.json});
    }
}

const LiveBusinessExtractOptions = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    expected_json: []const u8,
    limit: usize,
    batch_size: usize = 0,
    llm_parallel: usize = 1,
    provider: llm.ProviderConfig,
    temperature: f32,
    max_tokens: ?u32,
    context_budget_json: ?[]const u8 = null,
    request_output_path: ?[]const u8 = null,
    raw_output_path: ?[]const u8 = null,
};

fn buildBusinessExtractionSystemPrompt(
    allocator: Allocator,
    db: facet_sqlite.Database,
    opts: BusinessExtractionPromptOptions,
) ![]u8 {
    var system: std.Io.Writer.Allocating = .init(allocator);
    errdefer system.deinit();
    try system.writer.print(
        \\Extract the business graph described by the source documents.
        \\
        \\Use ontology_id "{s}".
        \\Use workspace_id "{s}".
        \\
        \\Return exactly one JSON object with these arrays:
        \\- entities_raw: rows with external_id, entity_type, name, confidence, metadata_json object
        \\- relations_raw: rows with external_id, edge_type, source_external_id, target_external_id, valid_from, valid_to, confidence, metadata_json object
        \\- entity_aliases_raw: rows with entity_external_id, term, confidence
        \\- entity_documents_raw: rows with entity_external_id, doc_id, role, confidence
        \\- entity_chunks_raw: rows with entity_external_id, doc_id, chunk_index, role, confidence
        \\- relation_properties_raw: rows with relation_external_id, property_key, value_type, value_text, value_number, value_integer, ref_doc_id, currency
        \\
        \\Do not return internal numeric IDs. Use stable external_id strings and reference those strings.
        \\
        \\Entity types and relation edge types must come from the syndic ontology vocabulary.
        \\Use canonical edge_type names from the ontology (for example "contains", "owns") rather than LinkML slot aliases.
        \\Extract entities and relations for every document listed in expected coverage, not only the first document.
        \\Preserve exact apartment identity: building, block, floor, lot, door_label, tantiemes, quota_basis.
        \\
        \\Ontology vocabulary:
        \\
    , .{ opts.ontology_id, opts.workspace_id });
    try appendQualificationVocabulary(&system.writer, allocator, db, opts.workspace_id, opts.collection_id, opts.ontology_id, "");
    return try system.toOwnedSlice();
}

fn callBusinessExtractLlm(
    allocator: Allocator,
    opts: LiveBusinessExtractOptions,
    prompt: PromptPair,
    write_debug_outputs: bool,
) ![]u8 {
    const messages = [_]llm.Message{
        .{ .role = "system", .content = prompt.system },
        .{ .role = "user", .content = prompt.user },
    };
    if (write_debug_outputs) {
        if (opts.request_output_path) |path| {
            const request_json = try renderChatRequestForDebug(allocator, opts.provider, &messages, .{
                .temperature = opts.temperature,
                .max_tokens = opts.max_tokens,
                .json_mode = true,
            });
            defer allocator.free(request_json);
            try std.Io.Dir.cwd().writeFile(mindbrain.zig16_compat.io(), .{
                .sub_path = path,
                .data = request_json,
                .flags = .{ .truncate = true },
            });
        }
    }

    const manager = llm.Manager.init(.{
        .providers = &.{opts.provider},
        .default_provider = "default",
    });
    var response = manager.chat(allocator, mindbrain.zig16_compat.io(), &messages, .{
        .temperature = opts.temperature,
        .max_tokens = opts.max_tokens,
        .json_mode = true,
    }) catch |err| {
        if (err == error.HttpRequestFailed) try writeLastLlmHttpFailure(allocator);
        return err;
    };
    defer response.deinit(allocator);

    if (write_debug_outputs) {
        if (opts.raw_output_path) |path| {
            try std.Io.Dir.cwd().writeFile(mindbrain.zig16_compat.io(), .{
                .sub_path = path,
                .data = response.raw_json,
                .flags = .{ .truncate = true },
            });
        }
    }

    const content = std.mem.trim(u8, response.content, " \t\r\n");
    if (content.len == 0) {
        try writeLlmBusinessExtractionFailure(allocator, .{
            .reason = "empty_content",
            .model = opts.provider.model,
            .content = response.content,
            .raw_json = response.raw_json,
            .parse_error = null,
        });
        return error.InvalidLlmBusinessExtractionResponse;
    }
    validateJsonObject(allocator, content) catch |err| {
        try writeLlmBusinessExtractionFailure(allocator, .{
            .reason = "invalid_json",
            .model = opts.provider.model,
            .content = response.content,
            .raw_json = response.raw_json,
            .parse_error = @errorName(err),
        });
        return error.InvalidLlmBusinessExtractionResponse;
    };
    const normalized = try sanitizeBusinessExtractionJson(allocator, content);
    return normalized;
}

fn parseBusinessExtractionEnvelopeJson(
    allocator: Allocator,
    json: []const u8,
    parse_opts: std.json.ParseOptions,
) !std.json.Parsed(BusinessExtractionEnvelope) {
    const normalized = try sanitizeBusinessExtractionJson(allocator, json);
    defer allocator.free(normalized);
    return std.json.parseFromSlice(BusinessExtractionEnvelope, allocator, normalized, parse_opts);
}

fn sanitizeBusinessExtractionJson(allocator: Allocator, json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    try sanitizeBusinessExtractionEnvelopeValue(allocator, &parsed.value);
    const out = try std.json.Stringify.valueAlloc(allocator, parsed.value, .{});
    parsed.deinit();
    return out;
}

fn ensureEnvelopeArrayField(allocator: Allocator, obj: *std.json.ObjectMap, key: []const u8) !void {
    const gop = try obj.getOrPut(allocator, key);
    if (gop.found_existing and gop.value_ptr.* == .array) return;
    gop.value_ptr.* = .{ .array = std.json.Array.init(allocator) };
}

fn copyJsonFieldAlias(
    allocator: Allocator,
    row_obj: *std.json.ObjectMap,
    dest: []const u8,
    aliases: []const []const u8,
) !void {
    if (row_obj.get(dest)) |_| return;
    for (aliases) |alias| {
        if (row_obj.get(alias)) |v| {
            if (v == .string) {
                try row_obj.put(allocator, dest, .{ .string = try allocator.dupe(u8, v.string) });
                return;
            }
        }
    }
}

fn jsonObjectHasNonEmptyString(obj: std.json.ObjectMap, key: []const u8) bool {
    const v = obj.get(key) orelse return false;
    return v == .string and v.string.len > 0;
}

fn sanitizeBusinessExtractionEnvelopeValue(allocator: Allocator, value: *std.json.Value) !void {
    if (value.* != .object) {
        value.* = .{ .object = .empty };
        return;
    }
    const obj = &value.object;

    for (&[_][]const u8{
        "entities_raw",
        "relations_raw",
        "entity_aliases_raw",
        "entity_documents_raw",
        "entity_chunks_raw",
        "relation_properties_raw",
    }) |key| {
        try ensureEnvelopeArrayField(allocator, obj, key);
    }

    if (obj.getPtr("entities_raw")) |rows_ptr| {
        if (rows_ptr.* == .array) {
            var i: usize = 0;
            while (i < rows_ptr.array.items.len) {
                const row = &rows_ptr.array.items[i];
                if (row.* != .object or !jsonObjectHasNonEmptyString(row.object, "external_id")) {
                    _ = rows_ptr.array.swapRemove(i);
                    continue;
                }
                const row_obj = &row.object;
                try copyJsonFieldAlias(allocator, row_obj, "entity_type", &.{ "type", "kind" });
                if (!jsonObjectHasNonEmptyString(row_obj.*, "entity_type")) {
                    try row_obj.put(allocator, "entity_type", .{ .string = "entity" });
                }
                if (!jsonObjectHasNonEmptyString(row_obj.*, "name")) {
                    const ext = row_obj.get("external_id").?.string;
                    try row_obj.put(allocator, "name", .{ .string = ext });
                }
                if (row_obj.getPtr("confidence")) |field| coerceJsonConfidence(field) else {
                    try row_obj.put(allocator, "confidence", .{ .float = 1.0 });
                }
                i += 1;
            }
        }
    }

    if (obj.getPtr("relations_raw")) |rows_ptr| {
        if (rows_ptr.* == .array) {
            var i: usize = 0;
            while (i < rows_ptr.array.items.len) {
                const row = &rows_ptr.array.items[i];
                if (row.* != .object) {
                    _ = rows_ptr.array.swapRemove(i);
                    continue;
                }
                const row_obj = &row.object;
                try copyJsonFieldAlias(allocator, row_obj, "source_external_id", &.{
                    "source_entity_id", "source_id", "from_external_id", "from",
                });
                try copyJsonFieldAlias(allocator, row_obj, "target_external_id", &.{
                    "target_entity_id", "target_id", "to_external_id", "to",
                });
                try copyJsonFieldAlias(allocator, row_obj, "edge_type", &.{ "relation_type", "type" });
                if (!jsonObjectHasNonEmptyString(row_obj.*, "source_external_id") or
                    !jsonObjectHasNonEmptyString(row_obj.*, "target_external_id") or
                    !jsonObjectHasNonEmptyString(row_obj.*, "edge_type"))
                {
                    _ = rows_ptr.array.swapRemove(i);
                    continue;
                }
                if (!jsonObjectHasNonEmptyString(row_obj.*, "external_id")) {
                    const edge = row_obj.get("edge_type").?.string;
                    const src = row_obj.get("source_external_id").?.string;
                    const tgt = row_obj.get("target_external_id").?.string;
                    const synth = try std.fmt.allocPrint(allocator, "{s}:{s}->{s}", .{ edge, src, tgt });
                    defer allocator.free(synth);
                    try row_obj.put(allocator, "external_id", .{ .string = synth });
                }
                if (row_obj.getPtr("confidence")) |field| coerceJsonConfidence(field) else {
                    try row_obj.put(allocator, "confidence", .{ .float = 1.0 });
                }
                i += 1;
            }
        }
    }

    if (obj.getPtr("entity_aliases_raw")) |rows_ptr| {
        if (rows_ptr.* == .array) {
            for (rows_ptr.array.items) |*row| {
                if (row.* != .object) continue;
                const row_obj = &row.object;
                try copyJsonFieldAlias(allocator, row_obj, "entity_external_id", &.{ "entity_id", "external_id" });
            }
        }
    }

    if (obj.getPtr("entity_documents_raw")) |rows_ptr| {
        if (rows_ptr.* == .array) {
            for (rows_ptr.array.items) |*row| {
                if (row.* != .object) continue;
                const row_obj = &row.object;
                try copyJsonFieldAlias(allocator, row_obj, "entity_external_id", &.{ "entity_id", "external_id" });
                if (row_obj.getPtr("doc_id")) |field| coerceJsonOptionalInteger(field);
            }
        }
    }

    if (obj.getPtr("entity_chunks_raw")) |rows_ptr| {
        if (rows_ptr.* == .array) {
            for (rows_ptr.array.items) |*row| {
                if (row.* != .object) continue;
                const row_obj = &row.object;
                try copyJsonFieldAlias(allocator, row_obj, "entity_external_id", &.{ "entity_id", "external_id" });
                if (row_obj.getPtr("doc_id")) |field| coerceJsonOptionalInteger(field);
                if (row_obj.getPtr("chunk_index")) |field| coerceJsonOptionalInteger(field);
            }
        }
    }

    if (obj.getPtr("relation_properties_raw")) |rows_ptr| {
        if (rows_ptr.* == .array) {
            for (rows_ptr.array.items) |*row| {
                if (row.* != .object) continue;
                const row_obj = &row.object;
                if (row_obj.getPtr("value_integer")) |field| coerceJsonOptionalInteger(field);
                if (row_obj.getPtr("ref_doc_id")) |field| coerceJsonOptionalInteger(field);
                if (row_obj.getPtr("value_number")) |field| coerceJsonOptionalFloat(field);
            }
        }
    }

    for (&[_][]const u8{ "entity_documents_raw", "entity_chunks_raw" }) |key| {
        const rows_ptr = obj.getPtr(key) orelse continue;
        if (rows_ptr.* != .array) continue;
        for (rows_ptr.array.items) |*row| {
            if (row.* != .object) continue;
            const row_obj = &row.object;
            if (row_obj.getPtr("doc_id")) |field| coerceJsonOptionalInteger(field);
            if (row_obj.getPtr("chunk_index")) |field| coerceJsonOptionalInteger(field);
        }
    }
}

fn coerceJsonConfidence(value: *std.json.Value) void {
    switch (value.*) {
        .float, .integer => {},
        .string => |s| {
            if (std.fmt.parseFloat(f64, s)) |n| {
                value.* = .{ .float = n };
            } else |_| {
                value.* = .{ .float = 1.0 };
            }
        },
        else => value.* = .{ .float = 1.0 },
    }
}

fn coerceJsonOptionalInteger(value: *std.json.Value) void {
    switch (value.*) {
        .integer => {},
        .float => |f| value.* = .{ .integer = @intFromFloat(f) },
        .string => |s| {
            if (s.len == 0) {
                value.* = .null;
            } else if (std.fmt.parseInt(i64, s, 10)) |n| {
                value.* = .{ .integer = n };
            } else |_| {
                value.* = .null;
            }
        },
        .null => {},
        else => value.* = .null,
    }
}

fn coerceJsonOptionalFloat(value: *std.json.Value) void {
    switch (value.*) {
        .float => {},
        .integer => |n| value.* = .{ .float = @floatFromInt(n) },
        .string => |s| {
            if (s.len == 0) {
                value.* = .null;
            } else if (std.fmt.parseFloat(f64, s)) |n| {
                value.* = .{ .float = n };
            } else |_| {
                value.* = .null;
            }
        },
        .null => {},
        else => value.* = .null,
    }
}

fn splitDocIdBatches(allocator: Allocator, doc_ids: []const u64, batch_size: usize) ![][]u64 {
    if (batch_size == 0 or doc_ids.len == 0) {
        return try allocator.dupe([]u64, &.{try allocator.dupe(u64, doc_ids)});
    }
    var batches = std.ArrayList([]u64).empty;
    errdefer {
        for (batches.items) |batch| allocator.free(batch);
        batches.deinit(allocator);
    }
    var start: usize = 0;
    while (start < doc_ids.len) {
        const end = @min(start + batch_size, doc_ids.len);
        try batches.append(allocator, try allocator.dupe(u64, doc_ids[start..end]));
        start = end;
    }
    return try batches.toOwnedSlice(allocator);
}

const ExtractBatchJob = struct {
    batch_index: usize,
    batch: []const u64,
    prompt: PromptPair,
    part: ?[]u8 = null,
    llm_err: ?anyerror = null,
};

const LlmWorkerCtx = struct {
    parent_allocator: Allocator,
    opts: *const LiveBusinessExtractOptions,
    job: *ExtractBatchJob,
    docs_rows: []const PromptDocumentRow,
    batch_total: usize,
};

fn businessExtractLlmWorker(ctx: *LlmWorkerCtx) void {
    var arena = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
    defer arena.deinit();
    const thread_alloc = arena.allocator();
    runOneExtractBatchLlm(thread_alloc, ctx.opts, ctx.job, ctx.docs_rows, ctx.batch_total) catch |err| {
        ctx.job.llm_err = err;
        return;
    };
    const part = ctx.job.part orelse return;
    const owned = ctx.parent_allocator.dupe(u8, part) catch |err| {
        ctx.job.llm_err = err;
        return;
    };
    ctx.job.part = owned;
}

fn runOneExtractBatchLlm(
    allocator: Allocator,
    opts: *const LiveBusinessExtractOptions,
    job: *ExtractBatchJob,
    docs_rows: []const PromptDocumentRow,
    batch_total: usize,
) !void {
    try logBusinessExtractBatchDocs("llm_start", job.batch_index, batch_total, job.batch, docs_rows);
    const part = try callBusinessExtractLlm(allocator, opts.*, job.prompt, job.batch_index == 0);
    job.part = part;
    const summary = summarizeBusinessExtractionEnvelope(part);
    const done_detail = try std.fmt.allocPrint(
        allocator,
        "entities={d} relations={d} entity_documents={d} json_chars={d}",
        .{ summary.entities, summary.relations, summary.entity_documents, part.len },
    );
    defer allocator.free(done_detail);
    for (job.batch) |doc_id| {
        const row = findPromptDocumentRow(docs_rows, doc_id) orelse continue;
        try writeBusinessExtractProgress("llm_done", job.batch_index, batch_total, doc_id, row.source_ref, row.content.len, done_detail);
    }
}

fn runExtractBatchLlmJobs(
    allocator: Allocator,
    opts: *const LiveBusinessExtractOptions,
    jobs: []ExtractBatchJob,
    docs_rows: []const PromptDocumentRow,
) !void {
    const parallel = @max(opts.llm_parallel, 1);
    const parallel_detail = try std.fmt.allocPrint(allocator, "llm_parallel={d}", .{parallel});
    defer allocator.free(parallel_detail);
    try writeBusinessExtractProgress("llm_parallelism", 0, jobs.len, 0, "", 0, parallel_detail);

    if (parallel == 1) {
        for (jobs) |*job| {
            try runOneExtractBatchLlm(allocator, opts, job, docs_rows, jobs.len);
        }
    } else {
        var wave_start: usize = 0;
        while (wave_start < jobs.len) {
            const wave_len = @min(parallel, jobs.len - wave_start);
            const ctxs = try allocator.alloc(LlmWorkerCtx, wave_len);
            defer allocator.free(ctxs);
            const threads = try allocator.alloc(std.Thread, wave_len);
            defer allocator.free(threads);

            for (0..wave_len) |w| {
                const job = &jobs[wave_start + w];
                ctxs[w] = .{
                    .parent_allocator = allocator,
                    .opts = opts,
                    .job = job,
                    .docs_rows = docs_rows,
                    .batch_total = jobs.len,
                };
                threads[w] = try std.Thread.spawn(.{}, businessExtractLlmWorker, .{&ctxs[w]});
            }
            for (threads[0..wave_len]) |*thread| {
                thread.join();
            }
            wave_start += wave_len;
        }
    }

    for (jobs) |*job| {
        if (job.llm_err) |err| return err;
        if (job.part == null) return error.InvalidLlmBusinessExtractionResponse;
    }
}

fn runLiveBusinessExtraction(allocator: Allocator, db: facet_sqlite.Database, opts: LiveBusinessExtractOptions) ![]u8 {
    const budget_ctx = PromptBudgetContext{
        .provider = budgetProviderName(opts.provider),
        .model = opts.provider.model,
        .context_budget_json = opts.context_budget_json,
    };
    const prompt_opts = BusinessExtractionPromptOptions{
        .workspace_id = opts.workspace_id,
        .collection_id = opts.collection_id,
        .ontology_id = opts.ontology_id,
        .expected_json = opts.expected_json,
        .limit = opts.limit,
    };

    var docs = try loadPromptDocuments(allocator, db, opts.workspace_id, opts.collection_id, opts.limit, null);
    defer docs.deinit(allocator);
    var doc_ids = try allocator.alloc(u64, docs.rows.len);
    defer allocator.free(doc_ids);
    for (docs.rows, 0..) |row, i| doc_ids[i] = row.doc_id;

    const batch_size = if (opts.batch_size > 0) opts.batch_size else docs.rows.len;
    const batch_detail = try std.fmt.allocPrint(allocator, "doc_count={d} batch_size={d}", .{ docs.rows.len, batch_size });
    defer allocator.free(batch_detail);
    try writeBusinessExtractProgress("docs_loaded", 0, docs.rows.len, 0, "", 0, batch_detail);
    for (docs.rows) |row| {
        try writeBusinessExtractProgress("doc_inventory", 0, docs.rows.len, row.doc_id, row.source_ref, row.content.len, "");
    }

    const batches = if (opts.batch_size > 0)
        try splitDocIdBatches(allocator, doc_ids, opts.batch_size)
    else blk: {
        const system = try buildBusinessExtractionSystemPrompt(allocator, db, prompt_opts);
        defer allocator.free(system);
        const user_prefix = try std.fmt.allocPrint(allocator, "Expected coverage contract (cover all listed documents):\n{s}\n\nSource documents:\n\n", .{opts.expected_json});
        defer allocator.free(user_prefix);
        var content_lengths = try allocator.alloc(usize, docs.rows.len);
        defer allocator.free(content_lengths);
        var metadata_total: usize = 0;
        for (docs.rows, 0..) |row, i| {
            content_lengths[i] = row.content.len;
            metadata_total += estimateBusinessExtractDocMetadataChars(row);
        }
        const spec = try llm_context_budget.loadBudget(allocator, budget_ctx.provider, budget_ctx.model, budget_ctx.context_budget_json);
        var budget_report = try llm_context_budget.computeDocumentCharLimits(
            allocator,
            spec,
            doc_ids,
            content_lengths,
            system.len,
            user_prefix.len + metadata_total,
        );
        budget_report.provider = budget_ctx.provider;
        budget_report.model = budget_ctx.model;
        defer budget_report.deinit(allocator);
        try llm_context_budget.writeContextBudgetReport(allocator, budget_report);
        if (budget_report.needs_batch) {
            const planned = try llm_context_budget.planBudgetBatches(allocator, spec, doc_ids, content_lengths, system.len, user_prefix.len + metadata_total);
            try llm_context_budget.writeContextBudgetBatchReport(allocator, planned);
            break :blk planned;
        }
        break :blk try allocator.dupe([]u64, &.{try allocator.dupe(u64, doc_ids)});
    };
    defer {
        for (batches) |batch| allocator.free(batch);
        allocator.free(batches);
    }

    const batches_detail = try std.fmt.allocPrint(allocator, "batch_count={d}", .{batches.len});
    defer allocator.free(batches_detail);
    try writeBusinessExtractProgress("batches_planned", 0, batches.len, 0, "", 0, batches_detail);

    if (batches.len == 1 and batches[0].len == doc_ids.len) {
        var prompt = try buildBusinessExtractionPrompts(allocator, db, prompt_opts, .{
            .provider = budget_ctx.provider,
            .model = budget_ctx.model,
            .context_budget_json = budget_ctx.context_budget_json,
            .write_budget_report = false,
        });
        defer prompt.deinit(allocator);
        return try callBusinessExtractLlm(allocator, opts, prompt, true);
    }

    var parts = std.ArrayList([]u8).empty;
    errdefer {
        for (parts.items) |part| allocator.free(part);
        parts.deinit(allocator);
    }

    var jobs = try allocator.alloc(ExtractBatchJob, batches.len);
    defer {
        for (jobs) |*job| job.prompt.deinit(allocator);
        allocator.free(jobs);
    }
    for (batches, 0..) |batch, batch_index| {
        try logBusinessExtractBatchDocs("prompt_build", batch_index, batches.len, batch, docs.rows);
        jobs[batch_index] = .{
            .batch_index = batch_index,
            .batch = batch,
            .prompt = try buildBusinessExtractionPrompts(allocator, db, prompt_opts, .{
                .provider = budget_ctx.provider,
                .model = budget_ctx.model,
                .context_budget_json = budget_ctx.context_budget_json,
                .doc_ids = batch,
                .write_budget_report = false,
            }),
        };
    }

    try runExtractBatchLlmJobs(allocator, &opts, jobs, docs.rows);
    for (jobs) |*job| {
        try parts.append(allocator, job.part.?);
        job.part = null;
    }
    try writeBusinessExtractProgress("merge_start", 0, parts.items.len, 0, "", 0, "");
    const merged = try mergeBusinessExtractionEnvelopes(allocator, parts.items);
    const merged_summary = summarizeBusinessExtractionEnvelope(merged);
    const merge_detail = try std.fmt.allocPrint(
        allocator,
        "entities={d} relations={d} entity_documents={d} json_chars={d}",
        .{ merged_summary.entities, merged_summary.relations, merged_summary.entity_documents, merged.len },
    );
    defer allocator.free(merge_detail);
    try writeBusinessExtractProgress("merge_done", 0, parts.items.len, 0, "", 0, merge_detail);
    return merged;
}

const BusinessExtractionPromptOptions = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    expected_json: []const u8,
    limit: usize,
};

fn buildBusinessExtractionPrompts(
    allocator: Allocator,
    db: facet_sqlite.Database,
    opts: BusinessExtractionPromptOptions,
    budget_ctx: PromptBudgetContext,
) !PromptPair {
    const system = try buildBusinessExtractionSystemPrompt(allocator, db, opts);
    errdefer allocator.free(system);

    const user_prefix = try std.fmt.allocPrint(allocator, "Expected coverage contract (cover all listed documents):\n{s}\n\nSource documents:\n\n", .{opts.expected_json});
    defer allocator.free(user_prefix);

    var docs = try loadPromptDocuments(allocator, db, opts.workspace_id, opts.collection_id, opts.limit, budget_ctx.doc_ids);
    defer docs.deinit(allocator);

    var budget = try computePromptDocumentLimits(
        allocator,
        budget_ctx,
        docs.rows,
        system.len,
        user_prefix.len,
        estimateBusinessExtractDocMetadataChars,
    );
    defer budget.report.deinit(allocator);
    if (budget_ctx.write_budget_report and budget_ctx.doc_ids == null) {
        try llm_context_budget.writeContextBudgetReport(allocator, budget.report);
    }

    var user: std.Io.Writer.Allocating = .init(allocator);
    errdefer user.deinit();
    try user.writer.writeAll(user_prefix);
    try appendBusinessExtractionDocumentsJsonWithLimits(&user.writer, docs.rows, budget.limits);
    return .{
        .system = system,
        .user = try user.toOwnedSlice(),
    };
}

test "business extraction prompt keeps ontology vocabulary in system and coverage contract in user" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try seedPromptSplitFixture(db);

    var prompt = try buildBusinessExtractionPrompts(std.testing.allocator, db, .{
        .workspace_id = "ws-prompt",
        .collection_id = "ws-prompt::docs",
        .ontology_id = "ws-prompt::core",
        .expected_json = "{\"must_cover\":[\"access-control\"]}",
        .limit = 1,
    }, .{});
    defer prompt.deinit(std.testing.allocator);

    try std.testing.expect(std.mem.indexOf(u8, prompt.system, "\"taxonomies\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.system, "GOVERNS") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "must_cover") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "Article 1. Operators") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt.user, "\"taxonomies\"") == null);
}

const BusinessExtractionEnvelope = struct {
    entities_raw: []BusinessEntityRow = &.{},
    relations_raw: []BusinessRelationRow = &.{},
    entity_aliases_raw: []BusinessAliasRow = &.{},
    entity_documents_raw: []BusinessEntityDocumentRow = &.{},
    entity_chunks_raw: []BusinessEntityChunkRow = &.{},
    relation_properties_raw: []BusinessRelationPropertyRow = &.{},
};

const BusinessEntityRow = struct {
    external_id: []const u8,
    entity_type: []const u8,
    name: []const u8,
    confidence: f64 = 1.0,
    metadata_json: ?std.json.Value = null,
};

const BusinessRelationRow = struct {
    external_id: []const u8,
    edge_type: []const u8,
    source_external_id: []const u8,
    target_external_id: []const u8,
    valid_from: ?[]const u8 = null,
    valid_to: ?[]const u8 = null,
    confidence: f64 = 1.0,
    metadata_json: ?std.json.Value = null,
};

const BusinessAliasRow = struct {
    entity_external_id: []const u8,
    term: []const u8,
    confidence: f64 = 1.0,
};

const BusinessEntityDocumentRow = struct {
    entity_external_id: []const u8,
    doc_id: i64,
    role: ?[]const u8 = null,
    evidence_kind: ?[]const u8 = null,
    confidence: f64 = 1.0,
};

const BusinessEntityChunkRow = struct {
    entity_external_id: []const u8,
    doc_id: i64,
    chunk_index: i64,
    role: ?[]const u8 = null,
    evidence_kind: ?[]const u8 = null,
    confidence: f64 = 1.0,
};

const BusinessRelationPropertyRow = struct {
    relation_external_id: []const u8,
    property_key: ?[]const u8 = null,
    property_name: ?[]const u8 = null,
    value_type: ?[]const u8 = null,
    value_text: ?[]const u8 = null,
    value_number: ?f64 = null,
    value_integer: ?i64 = null,
    value_json: ?std.json.Value = null,
    ref_doc_id: ?i64 = null,
    currency: ?[]const u8 = null,
};

const BusinessExtractionApplyOptions = struct {
    workspace_id: []const u8,
    collection_id: []const u8,
    ontology_id: []const u8,
    json: []const u8,
    expected_json: ?[]const u8 = null,
};

const BusinessExtractionApplySummary = struct {
    entities: usize = 0,
    relations: usize = 0,
    aliases: usize = 0,
    entity_documents: usize = 0,
    entity_chunks: usize = 0,
    relation_properties: usize = 0,
};

const BusinessExtractionApplyFailure = struct {
    reason: []const u8,
    error_name: []const u8,
    envelope_json: []const u8,
};

fn writeBusinessExtractionApplyFailure(allocator: Allocator, failure: BusinessExtractionApplyFailure) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, failure, .{});
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("BUSINESS_EXTRACTION_APPLY_FAILURE_JSON={s}\n", .{json});
    try stderr.flush();
}

fn applyBusinessExtractionEnvelope(allocator: Allocator, db: facet_sqlite.Database, opts: BusinessExtractionApplyOptions) !BusinessExtractionApplySummary {
    try db.exec("BEGIN");
    errdefer db.exec("ROLLBACK") catch {};
    const summary = applyBusinessExtractionEnvelopeImpl(allocator, db, opts) catch |err| {
        if (err != business_entity_normalize.NormalizeError.InvalidEntityType) {
            try writeBusinessExtractionApplyFailure(allocator, .{
                .reason = "apply_failed",
                .error_name = @errorName(err),
                .envelope_json = opts.json,
            });
        }
        return err;
    };
    try db.exec("COMMIT");
    return summary;
}

fn applyBusinessExtractionEnvelopeImpl(allocator: Allocator, db: facet_sqlite.Database, opts: BusinessExtractionApplyOptions) !BusinessExtractionApplySummary {
    var parsed = try std.json.parseFromSlice(BusinessExtractionEnvelope, allocator, opts.json, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    var ontology_types = try business_entity_normalize.loadOntologyEntityTypes(allocator, db, opts.ontology_id);
    defer {
        var type_it = ontology_types.keyIterator();
        while (type_it.next()) |key| allocator.free(key.*);
        ontology_types.deinit();
    }

    var entity_ids = std.StringHashMap(u64).init(allocator);
    defer entity_ids.deinit();
    var relation_ids = std.StringHashMap(u64).init(allocator);
    defer relation_ids.deinit();
    var summary = BusinessExtractionApplySummary{};

    for (parsed.value.entities_raw) |row| {
        if (row.external_id.len == 0) return CliError.InvalidArguments;
        const entity_type = business_entity_normalize.normalizeBusinessEntityType(allocator, &ontology_types, row.external_id, row.entity_type, row.name) catch |err| {
            if (err == business_entity_normalize.NormalizeError.InvalidEntityType) {
                try writeBusinessExtractionApplyFailure(allocator, .{
                    .reason = "invalid_entity_type",
                    .error_name = @errorName(err),
                    .envelope_json = opts.json,
                });
            }
            return err;
        };
        defer allocator.free(entity_type);
        const metadata_json = try jsonValueObjectString(allocator, row.metadata_json);
        defer allocator.free(metadata_json);
        const id = try collections_sqlite.upsertEntityRawAuto(db, .{
            .workspace_id = opts.workspace_id,
            .ontology_id = opts.ontology_id,
            .external_id = row.external_id,
            .entity_type = entity_type,
            .name = row.name,
            .confidence = row.confidence,
            .metadata_json = metadata_json,
        });
        try entity_ids.put(row.external_id, id);
        summary.entities += 1;
    }

    for (parsed.value.relations_raw) |row| {
        if (row.external_id.len == 0) return CliError.InvalidArguments;
        const canonical_edge_type = business_edge_normalize.normalizeBusinessEdgeType(row.edge_type);
        const source_id = try resolveBusinessRelationEndpoint(allocator, db, opts, &entity_ids, row, canonical_edge_type, .source);
        const target_id = try resolveBusinessRelationEndpoint(allocator, db, opts, &entity_ids, row, canonical_edge_type, .target);
        const metadata_json = try jsonValueObjectString(allocator, row.metadata_json);
        defer allocator.free(metadata_json);
        const id = try collections_sqlite.upsertRelationRawAuto(db, .{
            .workspace_id = opts.workspace_id,
            .ontology_id = opts.ontology_id,
            .external_id = row.external_id,
            .edge_type = canonical_edge_type,
            .source_entity_id = source_id,
            .target_entity_id = target_id,
            .valid_from = row.valid_from,
            .valid_to = row.valid_to,
            .confidence = row.confidence,
            .metadata_json = metadata_json,
        });
        try relation_ids.put(row.external_id, id);
        summary.relations += 1;
    }

    for (parsed.value.entity_aliases_raw) |row| {
        const entity_id = entity_ids.get(row.entity_external_id) orelse try collections_sqlite.selectEntityRawIdByExternalId(db, opts.workspace_id, row.entity_external_id);
        try collections_sqlite.upsertEntityAliasRaw(db, .{
            .workspace_id = opts.workspace_id,
            .entity_id = entity_id,
            .term = row.term,
            .confidence = row.confidence,
        });
        summary.aliases += 1;
    }

    for (parsed.value.entity_documents_raw) |row| {
        const entity_id = entity_ids.get(row.entity_external_id) orelse try collections_sqlite.selectEntityRawIdByExternalId(db, opts.workspace_id, row.entity_external_id);
        try collections_sqlite.linkEntityDocumentRaw(db, .{
            .workspace_id = opts.workspace_id,
            .entity_id = entity_id,
            .collection_id = opts.collection_id,
            .doc_id = std.math.cast(u64, row.doc_id) orelse return CliError.InvalidArguments,
            .role = row.role orelse row.evidence_kind,
            .confidence = row.confidence,
        });
        summary.entity_documents += 1;
    }

    for (parsed.value.entity_chunks_raw) |row| {
        const entity_id = entity_ids.get(row.entity_external_id) orelse try collections_sqlite.selectEntityRawIdByExternalId(db, opts.workspace_id, row.entity_external_id);
        try collections_sqlite.linkEntityChunkRaw(db, .{
            .workspace_id = opts.workspace_id,
            .entity_id = entity_id,
            .collection_id = opts.collection_id,
            .doc_id = std.math.cast(u64, row.doc_id) orelse return CliError.InvalidArguments,
            .chunk_index = std.math.cast(u32, row.chunk_index) orelse return CliError.InvalidArguments,
            .role = row.role orelse row.evidence_kind,
            .confidence = row.confidence,
        });
        summary.entity_chunks += 1;
    }

    for (parsed.value.relation_properties_raw) |row| {
        const relation_id = relation_ids.get(row.relation_external_id) orelse try collections_sqlite.selectRelationRawIdByExternalId(db, opts.workspace_id, row.relation_external_id);
        const property_key = row.property_key orelse row.property_name orelse return CliError.InvalidArguments;
        const value_type = if (row.value_type) |v| parseRelationPropertyValueType(v) orelse return CliError.InvalidArguments else collections_sqlite.RelationPropertyValueType.text;
        const value_text_from_json = if (row.value_text == null and row.value_json != null) try jsonValueString(allocator, row.value_json.?) else null;
        defer if (value_text_from_json) |v| allocator.free(v);
        try collections_sqlite.upsertRelationPropertyRaw(db, .{
            .workspace_id = opts.workspace_id,
            .relation_id = relation_id,
            .property_key = property_key,
            .value_type = value_type,
            .value_text = row.value_text orelse value_text_from_json,
            .value_number = row.value_number,
            .value_integer = row.value_integer,
            .ref_doc_id = if (row.ref_doc_id) |v| std.math.cast(u64, v) orelse return CliError.InvalidArguments else null,
            .currency = if (value_type == .money_minor) row.currency else null,
        });
        summary.relation_properties += 1;
    }

    return summary;
}

const BusinessRelationEndpointSide = enum { source, target };

fn resolveBusinessRelationEndpoint(
    allocator: Allocator,
    db: facet_sqlite.Database,
    opts: BusinessExtractionApplyOptions,
    entity_ids: *std.StringHashMap(u64),
    row: BusinessRelationRow,
    canonical_edge_type: []const u8,
    side: BusinessRelationEndpointSide,
) !u64 {
    const external_id = switch (side) {
        .source => row.source_external_id,
        .target => row.target_external_id,
    };
    if (entity_ids.get(external_id)) |id| return id;
    const id = collections_sqlite.selectEntityRawIdByExternalId(db, opts.workspace_id, external_id) catch |err| switch (err) {
        error.NotFound => try createPlaceholderEndpointEntity(allocator, db, opts, row, canonical_edge_type, side, external_id),
        else => return err,
    };
    try entity_ids.put(external_id, id);
    return id;
}

fn createPlaceholderEndpointEntity(
    allocator: Allocator,
    db: facet_sqlite.Database,
    opts: BusinessExtractionApplyOptions,
    row: BusinessRelationRow,
    canonical_edge_type: []const u8,
    side: BusinessRelationEndpointSide,
    external_id: []const u8,
) !u64 {
    const entity_type = try lookupRelationEndpointEntityType(allocator, db, opts.ontology_id, canonical_edge_type, side);
    defer allocator.free(entity_type);
    const name = try humanizeExternalId(allocator, external_id);
    defer allocator.free(name);
    const metadata_json = try std.fmt.allocPrint(
        allocator,
        "{{\"inferred_placeholder\":true,\"missing_external_id\":{f},\"relation_external_id\":{f},\"edge_type\":{f},\"endpoint_side\":{f}}}",
        .{
            std.json.fmt(external_id, .{}),
            std.json.fmt(row.external_id, .{}),
            std.json.fmt(canonical_edge_type, .{}),
            std.json.fmt(@tagName(side), .{}),
        },
    );
    defer allocator.free(metadata_json);
    return collections_sqlite.upsertEntityRawAuto(db, .{
        .workspace_id = opts.workspace_id,
        .ontology_id = opts.ontology_id,
        .external_id = external_id,
        .entity_type = entity_type,
        .name = name,
        .confidence = @min(row.confidence, 0.4),
        .metadata_json = metadata_json,
    });
}

fn lookupRelationEndpointEntityType(
    allocator: Allocator,
    db: facet_sqlite.Database,
    ontology_id: []const u8,
    edge_type: []const u8,
    side: BusinessRelationEndpointSide,
) ![]const u8 {
    const column = switch (side) {
        .source => "source_entity_type",
        .target => "target_entity_type",
    };
    var sql_buf: [256]u8 = undefined;
    const sql = try std.fmt.bufPrint(
        &sql_buf,
        "SELECT {s} FROM ontology_edge_types WHERE ontology_id = ?1 AND edge_type = ?2 LIMIT 1",
        .{column},
    );
    const stmt = try facet_sqlite.prepare(db, sql);
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, ontology_id);
    try facet_sqlite.bindText(stmt, 2, edge_type);
    const rc = facet_sqlite.c.sqlite3_step(stmt);
    if (rc == facet_sqlite.c.SQLITE_ROW and facet_sqlite.c.sqlite3_column_type(stmt, 0) != facet_sqlite.c.SQLITE_NULL) {
        return try facet_sqlite.dupeColumnText(allocator, stmt, 0);
    }
    if (rc == facet_sqlite.c.SQLITE_ROW or rc == facet_sqlite.c.SQLITE_DONE) {
        return try allocator.dupe(u8, "entity");
    }
    return error.StepFailed;
}

fn humanizeExternalId(allocator: Allocator, external_id: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    var capitalize_next = true;
    for (external_id) |ch| {
        if (ch == '_' or ch == '-' or ch == ':' or ch == '/') {
            if (out.items.len > 0 and out.items[out.items.len - 1] != ' ') try out.append(allocator, ' ');
            capitalize_next = true;
            continue;
        }
        const normalized = if (capitalize_next and ch >= 'a' and ch <= 'z') ch - 32 else ch;
        try out.append(allocator, normalized);
        capitalize_next = false;
    }
    return out.toOwnedSlice(allocator);
}

fn jsonValueObjectString(allocator: Allocator, value: ?std.json.Value) ![]const u8 {
    if (value) |v| return try jsonValueString(allocator, v);
    return try allocator.dupe(u8, "{}");
}

fn jsonValueString(allocator: Allocator, value: std.json.Value) ![]const u8 {
    return std.json.Stringify.valueAlloc(allocator, value, .{});
}

fn parseRelationPropertyValueType(value: []const u8) ?collections_sqlite.RelationPropertyValueType {
    if (std.mem.eql(u8, value, "text")) return .text;
    if (std.mem.eql(u8, value, "string")) return .text;
    if (std.mem.eql(u8, value, "date")) return .text;
    if (std.mem.eql(u8, value, "datetime")) return .text;
    if (std.mem.eql(u8, value, "number")) return .number;
    if (std.mem.eql(u8, value, "integer")) return .number;
    if (std.mem.eql(u8, value, "float")) return .number;
    if (std.mem.eql(u8, value, "decimal")) return .number;
    if (std.mem.eql(u8, value, "percentage")) return .percentage_bp;
    if (std.mem.eql(u8, value, "percentage_bp")) return .percentage_bp;
    if (std.mem.eql(u8, value, "money")) return .money_minor;
    if (std.mem.eql(u8, value, "currency")) return .money_minor;
    if (std.mem.eql(u8, value, "money_minor")) return .money_minor;
    if (std.mem.eql(u8, value, "date_unix")) return .date_unix;
    if (std.mem.eql(u8, value, "document_ref")) return .doc_ref;
    if (std.mem.eql(u8, value, "doc_ref")) return .doc_ref;
    if (std.mem.eql(u8, value, "url")) return .uri;
    if (std.mem.eql(u8, value, "uri")) return .uri;
    return null;
}

test "document business extraction applies symbolic external ids with sqlite-generated raw ids" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-business" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-business::core",
        .workspace_id = "ws-business",
        .name = "core",
    });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = "ws-business",
        .collection_id = "ws-business::docs",
        .name = "docs",
    });
    try collections_sqlite.ensureEdgeType(db, .{
        .ontology_id = "ws-business::core",
        .edge_type = "OWNS",
        .source_entity_type = "person",
        .target_entity_type = "unit",
    });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws-business::core", .entity_type = "person", .label = "Person" });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws-business::core", .entity_type = "unit", .label = "Unit" });
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = "ws-business",
        .collection_id = "ws-business::docs",
        .doc_id = 1,
        .content = "Alice owns apartment A1.",
    });
    try collections_sqlite.upsertChunkRaw(db, .{
        .workspace_id = "ws-business",
        .collection_id = "ws-business::docs",
        .doc_id = 1,
        .chunk_index = 0,
        .content = "Alice owns apartment A1.",
    });

    const json =
        \\{
        \\  "entities_raw": [
        \\    {"external_id":"person:alice","entity_type":"person","name":"Alice","metadata_json":{"kind":"owner"}},
        \\    {"external_id":"unit:a1","entity_type":"unit","name":"A1","metadata_json":{"lot":"A1"}}
        \\  ],
        \\  "relations_raw": [
        \\    {"external_id":"owns:alice:a1","edge_type":"OWNS","source_external_id":"person:alice","target_external_id":"unit:a1","confidence":0.9},
        \\    {"external_id":"owns:lena:a1","edge_type":"OWNS","source_external_id":"lena_peeters","target_external_id":"unit:a1","confidence":0.9}
        \\  ],
        \\  "entity_aliases_raw": [
        \\    {"entity_external_id":"person:alice","term":"Mme Alice"}
        \\  ],
        \\  "entity_documents_raw": [
        \\    {"entity_external_id":"person:alice","doc_id":1,"role":"mention"}
        \\  ],
        \\  "entity_chunks_raw": [
        \\    {"entity_external_id":"unit:a1","doc_id":1,"chunk_index":0,"role":"mention"}
        \\  ],
        \\  "relation_properties_raw": [
        \\    {"relation_external_id":"owns:alice:a1","property_key":"share_bp","value_type":"percentage_bp","value_integer":10000},
        \\    {"relation_external_id":"owns:alice:a1","property_key":"start_date","value_type":"string","value_text":"2025-09-01","currency":"EUR"}
        \\  ]
        \\}
    ;

    const summary = try applyBusinessExtractionEnvelope(std.testing.allocator, db, .{
        .workspace_id = "ws-business",
        .collection_id = "ws-business::docs",
        .ontology_id = "ws-business::core",
        .json = json,
    });
    try std.testing.expectEqual(@as(usize, 2), summary.entities);
    try std.testing.expectEqual(@as(usize, 2), summary.relations);
    try std.testing.expectEqual(@as(usize, 2), summary.relation_properties);

    const alice_id = try collections_sqlite.selectEntityRawIdByExternalId(db, "ws-business", "person:alice");
    const lena_id = try collections_sqlite.selectEntityRawIdByExternalId(db, "ws-business", "lena_peeters");
    const relation_id = try collections_sqlite.selectRelationRawIdByExternalId(db, "ws-business", "owns:alice:a1");
    try std.testing.expect(alice_id > 0);
    try std.testing.expect(lena_id > 0);
    try std.testing.expect(relation_id > 0);
}

test "applyBusinessExtractionEnvelope maps block_contains_unit alias" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-alias" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-alias::core",
        .workspace_id = "ws-alias",
        .name = "core",
    });
    try collections_sqlite.ensureEdgeType(db, .{
        .ontology_id = "ws-alias::core",
        .edge_type = "contains",
        .source_entity_type = "block",
        .target_entity_type = "unit",
    });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws-alias::core", .entity_type = "block", .label = "Block" });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws-alias::core", .entity_type = "unit", .label = "Unit" });

    const json =
        \\{
        \\  "entities_raw": [
        \\    {"external_id":"block:1","entity_type":"block","name":"Block 1"},
        \\    {"external_id":"unit:1","entity_type":"unit","name":"Unit 1"}
        \\  ],
        \\  "relations_raw": [
        \\    {"external_id":"rel:1","edge_type":"block_contains_unit","source_external_id":"block:1","target_external_id":"unit:1","confidence":0.9}
        \\  ]
        \\}
    ;

    _ = try applyBusinessExtractionEnvelope(std.testing.allocator, db, .{
        .workspace_id = "ws-alias",
        .collection_id = "ws-alias::docs",
        .ontology_id = "ws-alias::core",
        .json = json,
    });

    const stmt = try facet_sqlite.prepare(db, "SELECT edge_type FROM relations_raw WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-alias");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    const ptr = facet_sqlite.c.sqlite3_column_text(stmt, 0) orelse return error.StepFailed;
    const edge_type = ptr[0..@intCast(facet_sqlite.c.sqlite3_column_bytes(stmt, 0))];
    try std.testing.expectEqualStrings("contains", edge_type);
}

test "applyBusinessExtractionEnvelope surfaces apply failure and rolls back" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-fail" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-fail::core",
        .workspace_id = "ws-fail",
        .name = "core",
    });

    const json =
        \\{"entities_raw":[{"external_id":"","entity_type":"building","name":"Bad"}],"relations_raw":[]}
    ;
    const result = applyBusinessExtractionEnvelope(std.testing.allocator, db, .{
        .workspace_id = "ws-fail",
        .collection_id = "ws-fail::docs",
        .ontology_id = "ws-fail::core",
        .json = json,
    });
    try std.testing.expectError(CliError.InvalidArguments, result);

    const stmt = try facet_sqlite.prepare(db, "SELECT COUNT(*) FROM entities_raw WHERE workspace_id = ?1");
    defer facet_sqlite.finalize(stmt);
    try facet_sqlite.bindText(stmt, 1, "ws-fail");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(stmt) == facet_sqlite.c.SQLITE_ROW);
    try std.testing.expectEqual(@as(i64, 0), facet_sqlite.c.sqlite3_column_int64(stmt, 0));
}

test "applyBusinessExtractionEnvelope normalizes shared_space external_id prefix" {
    var db = try facet_sqlite.Database.openInMemory();
    defer db.close();
    try db.applyStandaloneSchema();
    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = "ws-shared" });
    try collections_sqlite.ensureOntology(db, .{
        .ontology_id = "ws-shared::core",
        .workspace_id = "ws-shared",
        .name = "core",
    });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws-shared::core", .entity_type = "shared_space", .label = "Shared space" });
    try collections_sqlite.ensureEntityType(db, .{ .ontology_id = "ws-shared::core", .entity_type = "shared_equipment", .label = "Shared equipment" });

    const json =
        \\{"entities_raw":[
        \\  {"external_id":"shared_space_hall","entity_type":"entity","name":"Hall"},
        \\  {"external_id":"shared_equipment_lift","entity_type":"entity","name":"Lift"}
        \\],"relations_raw":[]}
    ;

    _ = try applyBusinessExtractionEnvelope(std.testing.allocator, db, .{
        .workspace_id = "ws-shared",
        .collection_id = "ws-shared::docs",
        .ontology_id = "ws-shared::core",
        .json = json,
    });

    const hall_stmt = try facet_sqlite.prepare(db, "SELECT entity_type FROM entities_raw WHERE workspace_id = ?1 AND external_id = ?2");
    defer facet_sqlite.finalize(hall_stmt);
    try facet_sqlite.bindText(hall_stmt, 1, "ws-shared");
    try facet_sqlite.bindText(hall_stmt, 2, "shared_space_hall");
    try std.testing.expect(facet_sqlite.c.sqlite3_step(hall_stmt) == facet_sqlite.c.SQLITE_ROW);
    const hall_ptr = facet_sqlite.c.sqlite3_column_text(hall_stmt, 0) orelse return error.StepFailed;
    const hall_type = hall_ptr[0..@intCast(facet_sqlite.c.sqlite3_column_bytes(hall_stmt, 0))];
    try std.testing.expectEqualStrings("shared_space", hall_type);
}

test "sanitizeBusinessExtractionJson fills missing relation fields" {
    const raw =
        \\{"entities_raw":[],"relations_raw":[{"edge_type":"contains","source_entity_id":"a","target_entity_id":"b"}]}
    ;
    const normalized = try sanitizeBusinessExtractionJson(std.testing.allocator, raw);
    defer std.testing.allocator.free(normalized);

    var parsed = try std.json.parseFromSlice(BusinessExtractionEnvelope, std.testing.allocator, normalized, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.relations_raw.len);
    try std.testing.expectEqualStrings("a", parsed.value.relations_raw[0].source_external_id);
    try std.testing.expectEqualStrings("b", parsed.value.relations_raw[0].target_external_id);
}

test "sanitizeBusinessExtractionJson coerces string confidence" {
    const raw =
        \\{"entities_raw":[{"external_id":"e1","entity_type":"unit","name":"U1","confidence":"0.8"}],"relations_raw":[]}
    ;
    const normalized = try sanitizeBusinessExtractionJson(std.testing.allocator, raw);
    defer std.testing.allocator.free(normalized);

    var parsed = try std.json.parseFromSlice(BusinessExtractionEnvelope, std.testing.allocator, normalized, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), parsed.value.entities_raw[0].confidence, 0.001);
}

test "sanitizeBusinessExtractionJson coerces empty numeric strings" {
    const raw =
        \\{"entities_raw":[],"relations_raw":[],"relation_properties_raw":[{"relation_external_id":"r1","value_integer":"","value_number":"","ref_doc_id":1}]}
    ;
    const normalized = try sanitizeBusinessExtractionJson(std.testing.allocator, raw);
    defer std.testing.allocator.free(normalized);

    var parsed = try std.json.parseFromSlice(BusinessExtractionEnvelope, std.testing.allocator, normalized, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.relation_properties_raw.len);
    try std.testing.expect(parsed.value.relation_properties_raw[0].value_integer == null);
    try std.testing.expect(parsed.value.relation_properties_raw[0].value_number == null);
    try std.testing.expectEqual(@as(?i64, 1), parsed.value.relation_properties_raw[0].ref_doc_id);
}

test "mergeBusinessExtractionEnvelopes preserves entity_documents across doc ids" {
    const parts = [_][]const u8{
        \\{"entities_raw":[{"external_id":"entity:1","entity_type":"building","name":"One"}],"relations_raw":[],"entity_documents_raw":[{"entity_external_id":"entity:1","doc_id":1}]}
        \\{"entities_raw":[{"external_id":"entity:2","entity_type":"building","name":"Two"}],"relations_raw":[],"entity_documents_raw":[{"entity_external_id":"entity:2","doc_id":2}]}
        \\{"entities_raw":[{"external_id":"entity:3","entity_type":"building","name":"Three"}],"relations_raw":[],"entity_documents_raw":[{"entity_external_id":"entity:3","doc_id":3}]}
    };
    const merged = try mergeBusinessExtractionEnvelopes(std.testing.allocator, &parts);
    defer std.testing.allocator.free(merged);

    var parsed = try std.json.parseFromSlice(BusinessExtractionEnvelope, std.testing.allocator, merged, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.entity_documents_raw.len);

    var seen = [_]bool{ false, false, false };
    for (parsed.value.entity_documents_raw) |row| {
        const doc_id = @as(usize, @intCast(row.doc_id));
        try std.testing.expect(doc_id >= 1 and doc_id <= 3);
        seen[doc_id - 1] = true;
    }
    try std.testing.expect(seen[0] and seen[1] and seen[2]);
}

fn renderChatRequestForDebug(allocator: Allocator, provider: llm.ProviderConfig, messages: []const llm.Message, options: llm.ChatOptions) ![]u8 {
    const request = llm.ChatRequest{
        .messages = messages,
        .temperature = options.temperature,
        .max_tokens = options.max_tokens,
        .json_mode = options.json_mode,
    };
    return switch (provider.kind) {
        .anthropic => llm.anthropic.renderMessagesRequest(allocator, provider.model, request),
        .gemini => llm.gemini.client.renderGenerateContentRequest(allocator, request),
        else => llm.openai_compat.chat.renderRequest(allocator, provider.model, request),
    };
}

const LlmBusinessExtractionFailure = struct {
    reason: []const u8,
    model: []const u8,
    content: []const u8,
    raw_json: []const u8,
    parse_error: ?[]const u8 = null,
};

fn writeLlmBusinessExtractionFailure(allocator: Allocator, failure: LlmBusinessExtractionFailure) !void {
    const json = try std.json.Stringify.valueAlloc(allocator, failure, .{});
    defer allocator.free(json);
    var stderr_file_writer = std.Io.File.stderr().writer(mindbrain.zig16_compat.io(), &.{});
    const stderr = &stderr_file_writer.interface;
    try stderr.print("LLM_BUSINESS_EXTRACTION_FAILURE_JSON={s}\n", .{json});
    try stderr.flush();
}

fn validateJsonObject(allocator: Allocator, json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidResponse;
}

fn runOntologyImportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var input_path: ?[]const u8 = null;
    var name: ?[]const u8 = null;
    var materialize_graph = false;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--input")) input_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--name")) name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--materialize-graph")) materialize_graph = true else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or ontology_id == null or input_path == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const io = mindbrain.zig16_compat.io();
    var in_file = try std.Io.Dir.cwd().openFile(io, input_path.?, .{});
    defer in_file.close(io);
    var read_buf: [65536]u8 = undefined;
    var fr = in_file.reader(io, &read_buf);

    const summary = try owl2_import.importNTriplesReader(db, allocator, workspace_id.?, ontology_id.?, &fr.interface, .{
        .ontology_name = name,
        .materialize_graph = materialize_graph,
    });
    try writeStdout(
        "{{\"ontology_id\":{f},\"triples\":{},\"classes\":{},\"object_properties\":{},\"datatype_properties\":{},\"ontology_relations\":{},\"graph_relations\":{}}}\n",
        .{ std.json.fmt(summary.ontology_id, .{}), summary.triples, summary.classes, summary.object_properties, summary.datatype_properties, summary.ontology_relations, summary.graph_relations },
    );
}

fn runOntologyExportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var format: []const u8 = "ntriples";

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output")) output_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--format")) format = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or ontology_id == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const io = mindbrain.zig16_compat.io();

    if (std.mem.eql(u8, format, "ntriples")) {
        var write_buf: [65536]u8 = undefined;
        if (output_path) |path| {
            var out_file = try std.Io.Dir.cwd().createFile(io, path, .{});
            defer out_file.close(io);
            var fw = out_file.writer(io, &write_buf);
            try owl2_import.exportNTriplesWriter(db, ontology_id.?, &fw.interface);
            try fw.interface.flush();
        } else {
            var sfw = std.Io.File.stdout().writer(io, &write_buf);
            try owl2_import.exportNTriplesWriter(db, ontology_id.?, &sfw.interface);
            try sfw.interface.flush();
        }
        return;
    }

    if (std.mem.eql(u8, format, "bundle")) {
        if (workspace_id == null) return CliError.InvalidArguments;
        const payload = try collections_io.exportToJson(allocator, db, .{ .taxonomies = workspace_id.? });
        defer allocator.free(payload);
        if (output_path) |path| {
            try std.Io.Dir.cwd().writeFile(io, .{
                .sub_path = path,
                .data = payload,
                .flags = .{ .truncate = true },
            });
        } else {
            try writeStdout("{s}", .{payload});
        }
        return;
    }

    return CliError.InvalidArguments;
}

fn runOntologyCompileLinkmlCommand(allocator: Allocator, args: []const []const u8) !void {
    var workspace_id: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var input_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var ntriples_path: ?[]const u8 = null;
    var db_path: ?[]const u8 = null;
    var name: ?[]const u8 = null;
    var profile: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--input")) input_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output")) output_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ntriples")) ntriples_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--name")) name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--profile")) profile = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (workspace_id == null or ontology_id == null or input_path == null) return CliError.InvalidArguments;
    if (profile != null and !std.mem.eql(u8, profile.?, "syndic")) return CliError.InvalidArguments;

    var result = try linkml_interchange.compileLinkmlToBundle(allocator, .{
        .input_path = input_path.?,
        .workspace_id = workspace_id.?,
        .ontology_id = ontology_id.?,
        .ontology_name = name,
    });
    defer result.deinit(allocator);

    const io = mindbrain.zig16_compat.io();
    if (output_path) |path| {
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = path,
            .data = result.bundle_json,
            .flags = .{ .truncate = true },
        });
    } else {
        try writeStdout("{s}\n", .{result.bundle_json});
    }
    if (ntriples_path) |path| {
        try std.Io.Dir.cwd().writeFile(io, .{
            .sub_path = path,
            .data = result.ntriples,
            .flags = .{ .truncate = true },
        });
    }

    if (db_path) |path| {
        var db = try facet_sqlite.Database.open(path);
        defer db.close();
        try db.applyStandaloneSchema();
        try collections_sqlite.ensureWorkspace(db, .{
            .workspace_id = workspace_id.?,
            .domain_profile = if (profile != null) "syndic" else null,
        });
        try linkml_interchange.importCompiledBundle(db, allocator, result.bundle_json);
        if (profile != null) {
            try syndic_profile_seed.seedSyndicProfile(db, workspace_id.?, ontology_id.?);
        }
        try collections_sqlite.setDefaultOntology(db, workspace_id.?, ontology_id.?);
    }

    if (output_path != null) {
        try writeStdout(
            "{{\"ontology_id\":{f},\"entity_types\":{},\"edge_types\":{},\"enum_values\":{},\"triples\":{},\"imported\":{}}}\n",
            .{ std.json.fmt(ontology_id.?, .{}), result.entity_type_count, result.edge_type_count, result.enum_value_count, result.triple_count, db_path != null },
        );
    }
}

fn runOntologyExportLinkmlCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var bundle_path: ?[]const u8 = null;
    var ontology_id: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--input-bundle")) bundle_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output")) output_path = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (ontology_id == null or (db_path == null and bundle_path == null) or (db_path != null and bundle_path != null)) return CliError.InvalidArguments;

    const payload = if (bundle_path) |path| blk: {
        const bundle = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), path, allocator, .limited(64 * 1024 * 1024));
        defer allocator.free(bundle);
        break :blk try linkml_interchange.exportLinkmlFromBundleJson(allocator, bundle, ontology_id.?);
    } else blk: {
        var db = try facet_sqlite.Database.open(db_path.?);
        defer db.close();
        try db.applyStandaloneSchema();
        break :blk try linkml_interchange.exportLinkmlFromDb(allocator, db, ontology_id.?);
    };
    defer allocator.free(payload);

    if (output_path) |path| {
        try std.Io.Dir.cwd().writeFile(mindbrain.zig16_compat.io(), .{
            .sub_path = path,
            .data = payload,
            .flags = .{ .truncate = true },
        });
    } else {
        try writeStdout("{s}", .{payload});
    }
}

fn runBackupExportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var scope_name: []const u8 = "workspace";
    var include_vectors = true;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output")) output_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--scope")) scope_name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--no-vectors")) include_vectors = false else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null) return CliError.InvalidArguments;

    const scope: collections_io.Scope = if (std.mem.eql(u8, scope_name, "workspace"))
        .{ .workspace = workspace_id.? }
    else if (std.mem.eql(u8, scope_name, "taxonomies"))
        .{ .taxonomies = workspace_id.? }
    else if (std.mem.eql(u8, scope_name, "collection"))
        .{ .collection = .{ .workspace_id = workspace_id.?, .collection_id = collection_id orelse return CliError.InvalidArguments } }
    else
        return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const bundle = try collections_io.exportToJsonWithOptions(allocator, db, scope, .{ .include_vectors = include_vectors });
    defer allocator.free(bundle);

    if (output_path) |path| {
        try std.Io.Dir.cwd().writeFile(mindbrain.zig16_compat.io(), .{
            .sub_path = path,
            .data = bundle,
            .flags = .{ .truncate = true },
        });
    } else {
        try writeStdout("{s}\n", .{bundle});
    }
}

fn runBackupLoadCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var bundle_path: ?[]const u8 = null;
    var dry_run = false;
    var reindex_mode: enum { none, graph, all } = .none;
    var document_table_id: ?u64 = null;
    var collection_id: ?[]const u8 = null;
    var table_id: ?u64 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--bundle")) bundle_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--dry-run")) dry_run = true else if (std.mem.eql(u8, arg, "--reindex")) {
            const mode = try requireArg(args, &index);
            if (std.mem.eql(u8, mode, "none")) reindex_mode = .none else if (std.mem.eql(u8, mode, "graph")) reindex_mode = .graph else if (std.mem.eql(u8, mode, "all")) reindex_mode = .all else return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--document-table-id")) {
            const raw = try requireArg(args, &index);
            document_table_id = try std.fmt.parseInt(u64, raw, 10);
        } else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--table-id")) {
            const raw = try requireArg(args, &index);
            table_id = try std.fmt.parseInt(u64, raw, 10);
        } else return CliError.InvalidArguments;
    }
    if (db_path == null or bundle_path == null) return CliError.InvalidArguments;

    const buf = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), bundle_path.?, allocator, .unlimited);
    defer allocator.free(buf);

    if (dry_run) {
        const summary = try collections_io.summarizeBundleJson(allocator, buf);
        defer summary.deinit(allocator);
        try writeStdout("{f}\n", .{std.json.fmt(.{
            .kind = summary.kind,
            .schema_version = summary.schema_version,
            .scope = .{
                .kind = summary.scope_kind,
                .workspace_id = summary.workspace_id,
                .collection_id = summary.collection_id,
            },
            .counts = .{
                .workspaces = summary.workspace_count,
                .collections = summary.collection_count,
                .ontologies = summary.ontology_count,
                .ontology_values = summary.ontology_value_count,
                .documents = summary.document_count,
                .chunks = summary.chunk_count,
                .facet_tables = summary.facet_table_count,
                .relation_properties = summary.relation_property_count,
            },
            .reindex = @tagName(reindex_mode),
        }, .{})});
        return;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const summary = try collections_io.summarizeBundleJson(allocator, buf);
    defer summary.deinit(allocator);

    try collections_io.importBundleJson(db, allocator, buf);
    try writeStdout("loaded backup bundle from {s}\n", .{bundle_path.?});

    switch (reindex_mode) {
        .none => {},
        .graph => {
            const result = try reindex_http.reindexGraph(allocator, &db, summary.workspace_id, document_table_id);
            try writeStdout(
                "reindexed graph for workspace {s}: projected={d}\n",
                .{ summary.workspace_id, result.projected_count },
            );
        },
        .all => {
            const coll = collection_id orelse summary.collection_id orelse summary.facet_collection_id orelse return CliError.InvalidArguments;
            const tid = table_id orelse document_table_id orelse summary.facet_table_id orelse return CliError.InvalidArguments;
            const result = try reindex_http.reindexAll(allocator, &db, summary.workspace_id, coll, tid);
            try writeStdout(
                "reindexed all for workspace {s}: graph={d} facets={d} bm25={d}\n",
                .{ summary.workspace_id, result.graph_projected, result.facet_assignments, result.bm25_documents },
            );
        },
    }
}

fn runCollectionExportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output")) output_path = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null) return CliError.InvalidArguments;

    var forwarded = std.ArrayList([]const u8).empty;
    defer forwarded.deinit(allocator);
    try forwarded.appendSlice(allocator, &.{ "--db", db_path.?, "--workspace-id", workspace_id.? });
    if (collection_id) |coll| try forwarded.appendSlice(allocator, &.{ "--scope", "collection", "--collection-id", coll });
    if (output_path) |path| try forwarded.appendSlice(allocator, &.{ "--output", path });
    try runBackupExportCommand(allocator, forwarded.items);
}

fn runCollectionImportCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var bundle_path: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--bundle")) bundle_path = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or bundle_path == null) return CliError.InvalidArguments;

    try runBackupLoadCommand(allocator, &.{ "--db", db_path.?, "--bundle", bundle_path.? });
}

fn parseChunkerStrategy(value: []const u8) ?chunker.Strategy {
    if (std.mem.eql(u8, value, "fixed_token")) return .fixed_token;
    if (std.mem.eql(u8, value, "sentence")) return .sentence;
    if (std.mem.eql(u8, value, "paragraph")) return .paragraph;
    if (std.mem.eql(u8, value, "recursive_character")) return .recursive_character;
    if (std.mem.eql(u8, value, "structure_aware")) return .structure_aware;
    // semantic / late require embedding callbacks that the CLI cannot supply.
    return null;
}

const corpus_eval_cases = [_][]const u8{
    "legal_article",
    "legal_consolidated",
    "legal_amendment",
    "technical_doc",
    "business_rule",
};

const CorpusEvalCaseResult = struct {
    case_name: []const u8,
    splitter: []const u8,
    specialized_splitter: bool,
    chunk_count: usize,
    profile_matched: u32,
    profile_total: u32,
    profile_score: f64,
    chunk_matched: u32,
    chunk_total: u32,
    chunk_score: f64,
    overall_score: f64,
};

fn runCorpusEvalCommand(allocator: Allocator, args: []const []const u8) !void {
    var fixtures_dir: []const u8 = "fixtures/corpus_eval";
    var case_filter: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--fixtures")) fixtures_dir = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--case")) case_filter = try requireArg(args, &index) else return CliError.InvalidArguments;
    }

    var results = std.ArrayList(CorpusEvalCaseResult).empty;
    defer results.deinit(allocator);

    for (corpus_eval_cases) |case_name| {
        if (case_filter) |filter| {
            if (!std.mem.eql(u8, filter, case_name)) continue;
        }
        try results.append(allocator, try evalCorpusCase(allocator, fixtures_dir, case_name));
    }

    if (results.items.len == 0) return CliError.InvalidArguments;

    var overall_sum: f64 = 0;
    for (results.items) |result| overall_sum += result.overall_score;
    const payload = .{
        .case_count = results.items.len,
        .overall_score = overall_sum / @as(f64, @floatFromInt(results.items.len)),
        .cases = results.items,
    };
    try writeStdout("{f}\n", .{std.json.fmt(payload, .{})});
}

fn evalCorpusCase(
    allocator: Allocator,
    fixtures_dir: []const u8,
    case_name: []const u8,
) !CorpusEvalCaseResult {
    const source_path = try fixturePath(allocator, fixtures_dir, case_name, "source.txt");
    defer allocator.free(source_path);
    const profile_path = try fixturePath(allocator, fixtures_dir, case_name, "expected_profile.json");
    defer allocator.free(profile_path);
    const chunks_path = try fixturePath(allocator, fixtures_dir, case_name, "expected_chunks.json");
    defer allocator.free(chunks_path);

    const source = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), source_path, allocator, .unlimited);
    defer allocator.free(source);
    const profile_json = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), profile_path, allocator, .unlimited);
    defer allocator.free(profile_json);

    var expected_profile = try corpus_profile.parseJson(allocator, profile_json);
    defer expected_profile.deinit();
    const decision = chunking_policy.decide(expected_profile.value);

    const chunks = if (decision.requires_specialized_splitter)
        try legal_chunker.chunkLegal(allocator, source, .{ .profile = legalProfileFor(expected_profile.value.recommended_splitter), .max_chars = decision.options.max_chars, .min_chars = decision.options.min_chars })
    else
        try chunker.chunk(allocator, source, decision.options);
    defer chunker.freeChunks(allocator, chunks);

    var chunk_expectations = try loadOptionalChunkExpectations(allocator, chunks_path);
    defer chunk_expectations.deinit();

    const summary = corpus_eval.summarize(expected_profile.value, expected_profile.value, chunks, chunk_expectations.value);
    return .{
        .case_name = case_name,
        .splitter = @tagName(expected_profile.value.recommended_splitter),
        .specialized_splitter = decision.requires_specialized_splitter,
        .chunk_count = chunks.len,
        .profile_matched = summary.profile.matched,
        .profile_total = summary.profile.total,
        .profile_score = summary.profile.ratio(),
        .chunk_matched = summary.chunks.matched,
        .chunk_total = summary.chunks.total,
        .chunk_score = summary.chunks.ratio(),
        .overall_score = summary.overall(),
    };
}

fn loadOptionalChunkExpectations(
    allocator: Allocator,
    path: []const u8,
) !std.json.Parsed([]corpus_eval.ChunkExpectation) {
    const json = std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), path, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return corpus_eval.parseChunkExpectations(allocator, "[]"),
        else => return err,
    };
    defer allocator.free(json);
    return corpus_eval.parseChunkExpectations(allocator, json);
}

fn fixturePath(
    allocator: Allocator,
    fixtures_dir: []const u8,
    case_name: []const u8,
    filename: []const u8,
) ![]u8 {
    return std.fs.path.join(allocator, &.{ fixtures_dir, case_name, filename });
}

fn legalProfileFor(profile: corpus_profile.SplitterProfile) legal_chunker.LegalProfile {
    return switch (profile) {
        .legal_consolidated => .legal_consolidated,
        .legal_amendment => .legal_amendment,
        else => .legal_article,
    };
}

fn runDocumentNormalizeCommand(allocator: Allocator, args: []const []const u8) !void {
    var input_path: ?[]const u8 = null;
    var output_dir: ?[]const u8 = null;
    var languages_arg: ?[]const u8 = null;
    var split_by_language = false;
    var pdf_backend: document_normalize.PdfBackend = .auto;
    var html_backend: document_normalize.HtmlBackend = .pandoc;
    var deepseek_command: ?[]const u8 = null;
    var min_text_chars: usize = 128;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--input")) input_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--output-dir")) output_dir = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--languages")) languages_arg = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--split-by-language")) split_by_language = true else if (std.mem.eql(u8, arg, "--pdf-backend")) {
            const v = try requireArg(args, &index);
            pdf_backend = parseNormalizePdfBackend(v) orelse return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--ocr-backend")) {
            const v = try requireArg(args, &index);
            pdf_backend = parseNormalizeOcrBackend(v) orelse return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--html-backend")) {
            const v = try requireArg(args, &index);
            html_backend = parseNormalizeHtmlBackend(v) orelse return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--deepseek-command")) deepseek_command = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--min-text-chars")) {
            const v = try requireArg(args, &index);
            min_text_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else return CliError.InvalidArguments;
    }

    if (input_path == null or output_dir == null) return CliError.InvalidArguments;

    const languages = try parseLanguageList(allocator, languages_arg);
    defer freeLanguageList(allocator, languages);

    var result = try document_normalize.normalize(allocator, mindbrain.zig16_compat.io(), .{
        .input_path = input_path.?,
        .output_dir = output_dir.?,
        .languages = languages,
        .split_by_language = split_by_language,
        .pdf_backend = pdf_backend,
        .html_backend = html_backend,
        .deepseek_command = deepseek_command,
        .min_text_chars = min_text_chars,
    });
    defer result.deinit(allocator);

    try writeStdout("{f}\n", .{std.json.fmt(.{
        .input_path = result.input_path,
        .manifest_path = result.manifest_path,
        .source_kind = result.kind.label(),
        .outputs = result.outputs,
    }, .{})});
}

fn parseNormalizePdfBackend(value: []const u8) ?document_normalize.PdfBackend {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "pdftotext")) return .pdftotext;
    if (std.mem.eql(u8, value, "ocrmypdf")) return .ocrmypdf;
    if (std.mem.eql(u8, value, "deepseek")) return .deepseek;
    if (std.mem.eql(u8, value, "none")) return .none;
    return null;
}

fn parseNormalizeOcrBackend(value: []const u8) ?document_normalize.PdfBackend {
    if (std.mem.eql(u8, value, "ocrmypdf")) return .ocrmypdf;
    if (std.mem.eql(u8, value, "deepseek")) return .deepseek;
    if (std.mem.eql(u8, value, "none")) return .none;
    return null;
}

fn parseNormalizeHtmlBackend(value: []const u8) ?document_normalize.HtmlBackend {
    if (std.mem.eql(u8, value, "pandoc")) return .pandoc;
    if (std.mem.eql(u8, value, "builtin-strip")) return .builtin_strip;
    if (std.mem.eql(u8, value, "builtin_strip")) return .builtin_strip;
    return null;
}

fn parseLanguageList(allocator: Allocator, raw: ?[]const u8) ![]const []const u8 {
    if (raw == null or raw.?.len == 0) return try allocator.alloc([]const u8, 0);
    var out = std.ArrayList([]const u8).empty;
    errdefer {
        for (out.items) |lang| allocator.free(lang);
        out.deinit(allocator);
    }
    var it = std.mem.splitScalar(u8, raw.?, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, trimmed));
    }
    return out.toOwnedSlice(allocator);
}

fn freeLanguageList(allocator: Allocator, languages: []const []const u8) void {
    for (languages) |lang| allocator.free(lang);
    allocator.free(languages);
}

fn matchesIncludedExtension(path: []const u8, include_exts: []const []const u8) bool {
    if (include_exts.len == 0) return true;
    const ext = std.fs.path.extension(path);
    const normalized = if (ext.len > 0 and ext[0] == '.') ext[1..] else ext;
    for (include_exts) |candidate_raw| {
        const candidate = if (candidate_raw.len > 0 and candidate_raw[0] == '.') candidate_raw[1..] else candidate_raw;
        if (std.ascii.eqlIgnoreCase(normalized, candidate)) return true;
    }
    return false;
}

fn runDocumentProfileCommand(allocator: Allocator, args: []const []const u8, env_map: *const std.process.Environ.Map) !void {
    var content: ?[]const u8 = null;
    var content_file: ?[]const u8 = null;
    var content_dir: ?[]const u8 = null;
    var source_ref: ?[]const u8 = null;
    var llm_provider_arg: ?[]const u8 = null;
    var base_url: ?[]const u8 = null;
    var api_key: ?[]const u8 = null;
    var model: ?[]const u8 = null;
    var sample_chars: usize = 12_000;
    var temperature: f32 = 0.0;
    var max_tokens: u32 = 1200;
    var dry_run = false;
    var mock_profile_file: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--content")) content = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--content-file")) content_file = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--content-dir")) content_dir = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--source-ref")) source_ref = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--llm-provider")) llm_provider_arg = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--base-url")) base_url = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--api-key")) api_key = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--model")) model = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--sample-chars")) {
            const v = try requireArg(args, &index);
            sample_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--temperature")) {
            const v = try requireArg(args, &index);
            temperature = std.fmt.parseFloat(f32, v) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            const v = try requireArg(args, &index);
            max_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--mock-profile-json")) {
            mock_profile_file = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--dry-run")) dry_run = true else return CliError.InvalidArguments;
    }

    var input_count: u8 = 0;
    if (content != null) input_count += 1;
    if (content_file != null) input_count += 1;
    if (content_dir != null) input_count += 1;
    if (input_count != 1) return CliError.InvalidArguments;

    if (content_dir) |dir_path| {
        var loaded_env = try llm_provider.loadEnvValues(allocator, mindbrain.zig16_compat.io(), env_map);
        defer loaded_env.deinit(allocator);
        try runDocumentProfileDirectory(allocator, .{
            .dir_path = dir_path,
            .source_ref = source_ref,
            .llm_provider_arg = llm_provider_arg,
            .base_url = base_url,
            .api_key = api_key,
            .model = model,
            .env_values = loaded_env.values,
            .sample_chars = sample_chars,
            .temperature = temperature,
            .max_tokens = max_tokens,
            .dry_run = dry_run,
            .mock_profile_file = mock_profile_file,
        });
        return;
    }

    var loaded_content: ?[]u8 = null;
    defer if (loaded_content) |buf| allocator.free(buf);
    const text: []const u8 = if (content) |c| c else blk: {
        const buf = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), content_file.?, allocator, .unlimited);
        loaded_content = buf;
        break :blk buf;
    };

    var loaded_env = try llm_provider.loadEnvValues(allocator, mindbrain.zig16_compat.io(), env_map);
    defer loaded_env.deinit(allocator);
    const profile_json = try profileDocumentContent(allocator, .{
        .text = text,
        .source_ref = source_ref orelse content_file,
        .llm_provider_arg = llm_provider_arg,
        .base_url = base_url,
        .api_key = api_key,
        .model = model,
        .env_values = loaded_env.values,
        .sample_chars = sample_chars,
        .temperature = temperature,
        .max_tokens = max_tokens,
        .dry_run = dry_run,
        .mock_profile_file = mock_profile_file,
    });
    defer allocator.free(profile_json);
    try writeStdout("{s}\n", .{profile_json});
}

const DocumentProfileOptions = struct {
    text: []const u8,
    source_ref: ?[]const u8,
    llm_provider_arg: ?[]const u8,
    base_url: ?[]const u8,
    api_key: ?[]const u8,
    model: ?[]const u8,
    env_values: llm_provider.EnvValues,
    sample_chars: usize,
    temperature: f32,
    max_tokens: u32,
    dry_run: bool,
    mock_profile_file: ?[]const u8,
};

const DocumentProfileDirOptions = struct {
    dir_path: []const u8,
    source_ref: ?[]const u8,
    llm_provider_arg: ?[]const u8,
    base_url: ?[]const u8,
    api_key: ?[]const u8,
    model: ?[]const u8,
    env_values: llm_provider.EnvValues,
    sample_chars: usize,
    temperature: f32,
    max_tokens: u32,
    dry_run: bool,
    mock_profile_file: ?[]const u8,
};

const DocumentProfileJob = struct {
    content_path: []const u8,
    source_ref: []const u8,
    sample_chars: usize = 12_000,
    workspace_id: ?[]const u8 = null,
    collection_id: ?[]const u8 = null,
    doc_id: ?u64 = null,
    language: []const u8 = "english",
};

const NormalizedSidecar = struct {
    source_path: ?[]const u8 = null,
    language: ?[]const u8 = null,
};

fn profileDocumentContent(allocator: Allocator, opts: DocumentProfileOptions) ![]u8 {
    var prompt = try corpus_profile_prompt.build(allocator, opts.text, opts.source_ref, opts.sample_chars);
    defer prompt.deinit(allocator);
    const messages = prompt.messages();

    if (opts.dry_run) {
        return try std.json.Stringify.valueAlloc(allocator, .{
            .source_ref = opts.source_ref orelse "unknown",
            .messages = messages,
            .sample_chars = opts.sample_chars,
        }, .{});
    }

    if (opts.mock_profile_file) |path| {
        const profile_json = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), path, allocator, .unlimited);
        errdefer allocator.free(profile_json);
        var parsed = try corpus_profile.parseJson(allocator, profile_json);
        defer parsed.deinit();
        return profile_json;
    }

    const resolved_provider = try llm_provider.resolve(.{
        .provider = opts.llm_provider_arg,
        .base_url = opts.base_url,
        .api_key = opts.api_key,
        .model = opts.model,
    }, opts.env_values);
    const manager = llm.Manager.init(.{
        .providers = &.{resolved_provider.provider},
        .default_provider = "default",
    });

    var response = manager.chat(allocator, mindbrain.zig16_compat.io(), &messages, .{
        .temperature = opts.temperature,
        .max_tokens = opts.max_tokens,
        .json_mode = true,
    }) catch |err| {
        if (err == error.HttpRequestFailed) try writeLastLlmHttpFailure(allocator);
        return err;
    };
    defer response.deinit(allocator);

    var parsed = try corpus_profile.parseJson(allocator, response.content);
    defer parsed.deinit();
    return try allocator.dupe(u8, response.content);
}

fn runDocumentProfileEnqueueCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var queue_name: []const u8 = "document_profile";
    var content_file: ?[]const u8 = null;
    var content_dir: ?[]const u8 = null;
    var source_ref: ?[]const u8 = null;
    var sample_chars: usize = 12_000;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var doc_id: ?u64 = null;
    var doc_id_start: ?u64 = null;
    var language: []const u8 = "english";
    var include_ext_arg: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--queue")) queue_name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--content-file")) content_file = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--content-dir")) content_dir = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--source-ref")) source_ref = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--sample-chars")) {
            const v = try requireArg(args, &index);
            sample_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--doc-id")) {
            const v = try requireArg(args, &index);
            doc_id = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--doc-id-start")) {
            const v = try requireArg(args, &index);
            doc_id_start = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--language")) {
            language = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--include-ext")) {
            include_ext_arg = try requireArg(args, &index);
        } else return CliError.InvalidArguments;
    }

    if (db_path == null) return CliError.InvalidArguments;
    var input_count: u8 = 0;
    if (content_file != null) input_count += 1;
    if (content_dir != null) input_count += 1;
    if (input_count != 1) return CliError.InvalidArguments;
    if ((workspace_id != null or collection_id != null or doc_id != null or doc_id_start != null) and
        (workspace_id == null or collection_id == null))
    {
        return CliError.InvalidArguments;
    }
    if (content_file != null and doc_id_start != null) return CliError.InvalidArguments;
    if (content_dir != null and doc_id != null) return CliError.InvalidArguments;
    if (workspace_id != null and content_file != null and doc_id == null) return CliError.InvalidArguments;
    if (workspace_id != null and content_dir != null and doc_id_start == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();
    var queue = queue_sqlite.QueueStore.init(db, allocator);
    const include_exts = try parseLanguageList(allocator, include_ext_arg);
    defer freeLanguageList(allocator, include_exts);

    var enqueued: usize = 0;
    if (content_file) |path| {
        _ = try enqueueProfilePath(allocator, &queue, queue_name, .{
            .path = path,
            .source_ref = source_ref,
            .sample_chars = sample_chars,
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .doc_id = doc_id,
            .language = language,
        });
        enqueued += 1;
    } else if (content_dir) |dir_path| {
        var files = std.ArrayList([]u8).empty;
        defer {
            for (files.items) |path| allocator.free(path);
            files.deinit(allocator);
        }
        try collectRegularFilesRecursive(allocator, dir_path, &files);
        std.mem.sort([]u8, files.items, {}, stringSliceLessThan);
        var next_doc_id = doc_id_start;
        for (files.items) |path| {
            if (!matchesIncludedExtension(path, include_exts)) continue;
            _ = try enqueueProfilePath(allocator, &queue, queue_name, .{
                .path = path,
                .source_ref = null,
                .sample_chars = sample_chars,
                .workspace_id = workspace_id,
                .collection_id = collection_id,
                .doc_id = next_doc_id,
                .language = language,
            });
            if (next_doc_id) |id| next_doc_id = id + 1;
            enqueued += 1;
        }
    }

    try writeStdout("{f}\n", .{std.json.fmt(.{ .queue = queue_name, .enqueued = enqueued }, .{})});
}

const EnqueueProfilePathOptions = struct {
    path: []const u8,
    source_ref: ?[]const u8,
    sample_chars: usize,
    workspace_id: ?[]const u8,
    collection_id: ?[]const u8,
    doc_id: ?u64,
    language: []const u8,
};

fn enqueueProfilePath(
    allocator: Allocator,
    queue: *queue_sqlite.QueueStore,
    queue_name: []const u8,
    opts: EnqueueProfilePathOptions,
) !i64 {
    var sidecar = try loadNormalizedSidecar(allocator, opts.path);
    defer if (sidecar) |*parsed| parsed.deinit();
    const sidecar_value: ?NormalizedSidecar = if (sidecar) |parsed| parsed.value else null;
    return try queue.sendJson(queue_name, DocumentProfileJob{
        .content_path = opts.path,
        .source_ref = opts.source_ref orelse if (sidecar_value) |s| s.source_path orelse opts.path else opts.path,
        .sample_chars = opts.sample_chars,
        .workspace_id = opts.workspace_id,
        .collection_id = opts.collection_id,
        .doc_id = opts.doc_id,
        .language = if (sidecar_value) |s| s.language orelse opts.language else opts.language,
    });
}

fn loadNormalizedSidecar(
    allocator: Allocator,
    path: []const u8,
) !?std.json.Parsed(NormalizedSidecar) {
    const sidecar_path = try std.fmt.allocPrint(allocator, "{s}.metadata.json", .{path});
    defer allocator.free(sidecar_path);
    const json = std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), sidecar_path, allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(json);
    return try std.json.parseFromSlice(NormalizedSidecar, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn runDocumentProfileWorkerCommand(allocator: Allocator, args: []const []const u8, env_map: *const std.process.Environ.Map) !void {
    var db_path: ?[]const u8 = null;
    var queue_name: []const u8 = "document_profile";
    var visibility_timeout: i64 = 300;
    var limit: usize = 1;
    var llm_provider_arg: ?[]const u8 = null;
    var base_url: ?[]const u8 = null;
    var api_key: ?[]const u8 = null;
    var model: ?[]const u8 = null;
    var temperature: f32 = 0.0;
    var max_tokens: u32 = 1200;
    var mock_profile_file: ?[]const u8 = null;
    var archive_failures = false;
    var contextual_retrieval = false;
    var contextual_doc_chars: usize = 60_000;
    var contextual_max_tokens: u32 = 180;
    var contextual_search_table_id: ?u64 = null;
    var embedding_base_url: ?[]const u8 = null;
    var embedding_api_key: ?[]const u8 = null;
    var embedding_model: ?[]const u8 = null;
    var contextual_chunk_bits: u6 = 8;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--queue")) queue_name = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--vt")) {
            const v = try requireArg(args, &index);
            visibility_timeout = std.fmt.parseInt(i64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--limit")) {
            const v = try requireArg(args, &index);
            limit = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--llm-provider")) llm_provider_arg = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--base-url")) base_url = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--api-key")) api_key = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--model")) model = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--temperature")) {
            const v = try requireArg(args, &index);
            temperature = std.fmt.parseFloat(f32, v) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            const v = try requireArg(args, &index);
            max_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--mock-profile-json")) mock_profile_file = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--archive-failures")) archive_failures = true else if (std.mem.eql(u8, arg, "--contextual-retrieval")) contextual_retrieval = true else if (std.mem.eql(u8, arg, "--contextual-doc-chars")) {
            const v = try requireArg(args, &index);
            contextual_doc_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--contextual-max-tokens")) {
            const v = try requireArg(args, &index);
            contextual_max_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--contextual-search-table-id")) {
            const v = try requireArg(args, &index);
            contextual_search_table_id = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--embedding-base-url")) {
            embedding_base_url = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--embedding-api-key")) {
            embedding_api_key = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--embedding-model")) {
            embedding_model = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--contextual-chunk-bits")) {
            const v = try requireArg(args, &index);
            const parsed = std.fmt.parseInt(u8, v, 10) catch return CliError.InvalidArguments;
            if (parsed > 63) return CliError.InvalidArguments;
            contextual_chunk_bits = @intCast(parsed);
        } else return CliError.InvalidArguments;
    }

    if (db_path == null) return CliError.InvalidArguments;
    var loaded_env = try llm_provider.loadEnvValues(allocator, mindbrain.zig16_compat.io(), env_map);
    defer loaded_env.deinit(allocator);
    if (contextual_retrieval and (contextual_search_table_id == null or embedding_model == null)) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();
    var queue = queue_sqlite.QueueStore.init(db, allocator);

    const messages = try queue.read(queue_name, visibility_timeout, limit);
    defer {
        for (messages) |msg| allocator.free(msg.message);
        allocator.free(messages);
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeByte('[');
    for (messages, 0..) |message, i| {
        if (i > 0) try out.writer.writeByte(',');
        try processProfileQueueMessage(allocator, &queue, queue_name, message, .{
            .base_url = base_url,
            .api_key = api_key,
            .model = model,
            .llm_provider_arg = llm_provider_arg,
            .env_values = loaded_env.values,
            .temperature = temperature,
            .max_tokens = max_tokens,
            .mock_profile_file = mock_profile_file,
            .archive_failures = archive_failures,
            .contextual_retrieval = contextual_retrieval,
            .contextual_doc_chars = contextual_doc_chars,
            .contextual_max_tokens = contextual_max_tokens,
            .contextual_search_table_id = contextual_search_table_id,
            .embedding_base_url = embedding_base_url,
            .embedding_api_key = embedding_api_key,
            .embedding_model = embedding_model,
            .contextual_chunk_bits = contextual_chunk_bits,
        }, &out);
    }
    try out.writer.writeByte(']');
    const body = try out.toOwnedSlice();
    defer allocator.free(body);
    try writeStdout("{s}\n", .{body});
}

const ProfileWorkerOptions = struct {
    base_url: ?[]const u8,
    api_key: ?[]const u8,
    model: ?[]const u8,
    llm_provider_arg: ?[]const u8,
    env_values: llm_provider.EnvValues,
    temperature: f32,
    max_tokens: u32,
    mock_profile_file: ?[]const u8,
    archive_failures: bool,
    contextual_retrieval: bool,
    contextual_doc_chars: usize,
    contextual_max_tokens: u32,
    contextual_search_table_id: ?u64,
    embedding_base_url: ?[]const u8,
    embedding_api_key: ?[]const u8,
    embedding_model: ?[]const u8,
    contextual_chunk_bits: u6,
};

fn processProfileQueueMessage(
    allocator: Allocator,
    queue: *queue_sqlite.QueueStore,
    queue_name: []const u8,
    message: queue_sqlite.Message,
    opts: ProfileWorkerOptions,
    out: *std.Io.Writer.Allocating,
) !void {
    var parsed_job = std.json.parseFromSlice(DocumentProfileJob, allocator, message.message, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| {
        if (opts.archive_failures) _ = try queue.archive(queue_name, message.msg_id);
        try out.writer.print("{f}", .{std.json.fmt(.{ .msg_id = message.msg_id, .ok = false, .@"error" = @errorName(err) }, .{})});
        return;
    };
    defer parsed_job.deinit();
    const job = parsed_job.value;

    const text = std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), job.content_path, allocator, .unlimited) catch |err| {
        if (opts.archive_failures) _ = try queue.archive(queue_name, message.msg_id);
        try out.writer.print("{f}", .{std.json.fmt(.{ .msg_id = message.msg_id, .source_ref = job.source_ref, .ok = false, .@"error" = @errorName(err) }, .{})});
        return;
    };
    defer allocator.free(text);

    const profile_json = profileDocumentContent(allocator, .{
        .text = text,
        .source_ref = job.source_ref,
        .base_url = opts.base_url,
        .api_key = opts.api_key,
        .model = opts.model,
        .llm_provider_arg = opts.llm_provider_arg,
        .env_values = opts.env_values,
        .sample_chars = job.sample_chars,
        .temperature = opts.temperature,
        .max_tokens = opts.max_tokens,
        .dry_run = false,
        .mock_profile_file = opts.mock_profile_file,
    }) catch |err| {
        if (opts.archive_failures) _ = try queue.archive(queue_name, message.msg_id);
        try out.writer.print("{f}", .{std.json.fmt(.{ .msg_id = message.msg_id, .source_ref = job.source_ref, .ok = false, .@"error" = @errorName(err) }, .{})});
        return;
    };
    defer allocator.free(profile_json);

    var parsed_profile_value = try std.json.parseFromSlice(std.json.Value, allocator, profile_json, .{});
    defer parsed_profile_value.deinit();
    var parsed_profile = try corpus_profile.parseJson(allocator, profile_json);
    defer parsed_profile.deinit();

    var persisted = false;
    var chunk_count: usize = 0;
    if (job.workspace_id != null or job.collection_id != null or job.doc_id != null) {
        if (job.workspace_id == null or job.collection_id == null or job.doc_id == null) return CliError.InvalidArguments;
        const persisted_result = try persistProfiledDocument(
            allocator,
            queue.db,
            job,
            text,
            parsed_profile.value,
            parsed_profile_value.value,
            .{
                .enabled = opts.contextual_retrieval,
                .base_url = opts.base_url,
                .api_key = opts.api_key,
                .model = opts.model,
                .temperature = 0.0,
                .max_tokens = opts.contextual_max_tokens,
                .max_doc_chars = opts.contextual_doc_chars,
                .search_table_id = opts.contextual_search_table_id,
                .embedding_base_url = opts.embedding_base_url orelse opts.base_url,
                .embedding_api_key = opts.embedding_api_key orelse opts.api_key,
                .embedding_model = opts.embedding_model,
                .chunk_bits = opts.contextual_chunk_bits,
            },
        );
        persisted = true;
        chunk_count = persisted_result.chunk_count;
    }

    _ = try queue.archive(queue_name, message.msg_id);
    try out.writer.print("{f}", .{std.json.fmt(.{
        .msg_id = message.msg_id,
        .source_ref = job.source_ref,
        .ok = true,
        .persisted = persisted,
        .chunk_count = chunk_count,
        .profile = parsed_profile_value.value,
    }, .{})});
}

const PersistProfiledDocumentResult = struct {
    chunk_count: usize,
};

const ContextualRetrievalOptions = struct {
    enabled: bool = false,
    base_url: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
    model: ?[]const u8 = null,
    temperature: f32 = 0.0,
    max_tokens: u32 = 180,
    max_doc_chars: usize = 60_000,
    search_table_id: ?u64 = null,
    embedding_base_url: ?[]const u8 = null,
    embedding_api_key: ?[]const u8 = null,
    embedding_model: ?[]const u8 = null,
    chunk_bits: u6 = 8,
};

const ChunkContextualMetadata = struct {
    context: []const u8,
    contextualized_content: []const u8,
    doc_context_chars: usize,
};

fn persistProfiledDocument(
    allocator: Allocator,
    db: facet_sqlite.Database,
    job: DocumentProfileJob,
    text: []const u8,
    profile: corpus_profile.DocumentProfile,
    profile_value: std.json.Value,
    contextual_options: ContextualRetrievalOptions,
) !PersistProfiledDocumentResult {
    const workspace_id = job.workspace_id orelse return CliError.InvalidArguments;
    const collection_id = job.collection_id orelse return CliError.InvalidArguments;
    const doc_id = job.doc_id orelse return CliError.InvalidArguments;

    try collections_sqlite.ensureWorkspace(db, .{ .workspace_id = workspace_id });
    try collections_sqlite.ensureCollection(db, .{
        .workspace_id = workspace_id,
        .collection_id = collection_id,
        .name = collection_id,
        .default_language = job.language,
    });

    const decision = chunking_policy.decide(profile);
    const metadata_json = try profileMetadataJson(allocator, profile_value, decision);
    defer allocator.free(metadata_json);
    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = workspace_id,
        .collection_id = collection_id,
        .doc_id = doc_id,
        .content = text,
        .language = job.language,
        .source_ref = job.source_ref,
        .metadata_json = metadata_json,
    });

    const chunks = if (decision.requires_specialized_splitter)
        try legal_chunker.chunkLegal(allocator, text, .{
            .profile = legalProfileFor(decision.profile),
            .max_chars = decision.options.max_chars,
            .min_chars = decision.options.min_chars,
        })
    else
        try chunker.chunk(allocator, text, decision.options);
    defer chunker.freeChunks(allocator, chunks);

    for (chunks) |ch| {
        const contextual = if (contextual_options.enabled)
            try contextualizeChunk(allocator, text, job.source_ref, profile, decision, ch, contextual_options)
        else
            null;
        defer if (contextual) |ctx| {
            allocator.free(ctx.context);
            allocator.free(ctx.contextualized_content);
        };
        const chunk_metadata_json = try chunkingMetadataJson(allocator, decision, contextual);
        defer allocator.free(chunk_metadata_json);
        try collections_sqlite.upsertChunkRaw(db, .{
            .workspace_id = workspace_id,
            .collection_id = collection_id,
            .doc_id = doc_id,
            .chunk_index = ch.index,
            .content = ch.content,
            .language = job.language,
            .offset_start = ch.offset_start,
            .offset_end = ch.offset_end,
            .strategy = ch.strategy,
            .token_count = ch.token_count,
            .parent_chunk_index = ch.parent_chunk_index,
            .metadata_json = chunk_metadata_json,
        });

        if (contextual) |ctx| {
            try indexContextualChunk(
                allocator,
                db,
                workspace_id,
                collection_id,
                doc_id,
                job.language,
                ch.index,
                ctx.contextualized_content,
                contextual_options,
            );
        }
    }

    return .{ .chunk_count = chunks.len };
}

fn indexContextualChunk(
    allocator: Allocator,
    db: facet_sqlite.Database,
    workspace_id: []const u8,
    collection_id: []const u8,
    doc_id: u64,
    language: []const u8,
    chunk_index: u32,
    contextualized_content: []const u8,
    options: ContextualRetrievalOptions,
) !void {
    if (!options.enabled) return;
    const table_id = options.search_table_id orelse return CliError.InvalidArguments;
    const synthetic_id = chunkSyntheticId(doc_id, chunk_index, options.chunk_bits);

    try search_sqlite.syncSearchDocument(
        db,
        allocator,
        table_id,
        synthetic_id,
        contextualized_content,
        language,
    );

    const embedding = try embedContextualText(allocator, contextualized_content, options);
    defer allocator.free(embedding);

    const blob = try vector_blob.encodeF32Le(allocator, embedding);
    defer allocator.free(blob);
    try collections_sqlite.upsertChunkVector(db, .{
        .workspace_id = workspace_id,
        .collection_id = collection_id,
        .doc_id = doc_id,
        .chunk_index = chunk_index,
        .dim = @intCast(embedding.len),
        .embedding_blob = blob,
    });
    try search_sqlite.upsertSearchEmbedding(db, allocator, table_id, synthetic_id, embedding);
}

fn embedContextualText(
    allocator: Allocator,
    text: []const u8,
    options: ContextualRetrievalOptions,
) ![]f32 {
    const provider = llm.ProviderConfig{
        .name = "contextual-embedding",
        .kind = .openai_compatible,
        .base_url = options.embedding_base_url orelse return CliError.InvalidArguments,
        .api_key = options.embedding_api_key,
        .model = options.embedding_model orelse return CliError.InvalidArguments,
        .capabilities = &.{.embeddings},
    };
    const manager = llm.Manager.init(.{
        .providers = &.{provider},
        .default_provider = "contextual-embedding",
    });

    var response = try manager.embedTexts(allocator, mindbrain.zig16_compat.io(), null, &.{text});
    defer response.deinit(allocator);
    if (response.vectors.len != 1) return error.InvalidResponse;
    return try allocator.dupe(f32, response.vectors[0].values);
}

fn chunkSyntheticId(doc_id: u64, chunk_index: u32, chunk_bits: u6) u64 {
    return (doc_id << chunk_bits) | @as(u64, chunk_index);
}

fn runContextualSearchCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var table_id: ?u64 = null;
    var query: ?[]const u8 = null;
    var base_url: ?[]const u8 = null;
    var api_key: ?[]const u8 = null;
    var embedding_model: ?[]const u8 = null;
    var limit: usize = 20;
    var vector_weight: f64 = 0.5;
    var rerank_options = RerankOptions{};

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--table-id")) {
            const v = try requireArg(args, &index);
            table_id = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--query")) query = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--base-url")) base_url = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--api-key")) api_key = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--embedding-model")) embedding_model = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--limit")) {
            const v = try requireArg(args, &index);
            limit = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--vector-weight")) {
            const v = try requireArg(args, &index);
            vector_weight = std.fmt.parseFloat(f64, v) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--rerank")) {
            rerank_options.enabled = true;
        } else if (std.mem.eql(u8, arg, "--rerank-base-url")) {
            rerank_options.base_url = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--rerank-api-key")) {
            rerank_options.api_key = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--rerank-model")) {
            rerank_options.model = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--rerank-provider-kind")) {
            rerank_options.provider_kind = parseProviderKind(try requireArg(args, &index)) orelse return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--rerank-candidates")) {
            const v = try requireArg(args, &index);
            rerank_options.candidate_limit = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--rerank-max-doc-chars")) {
            const v = try requireArg(args, &index);
            rerank_options.max_doc_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else return CliError.InvalidArguments;
    }

    if (db_path == null or table_id == null or query == null) return CliError.InvalidArguments;
    if (limit == 0 or vector_weight < 0.0 or vector_weight > 1.0) return CliError.InvalidArguments;
    const embedding_requested = base_url != null or api_key != null or embedding_model != null;
    if (embedding_requested and (base_url == null or embedding_model == null)) return CliError.InvalidArguments;
    if (rerank_options.enabled and (rerank_options.base_url == null or rerank_options.model == null)) return CliError.InvalidArguments;
    if (rerank_options.enabled and (rerank_options.candidate_limit == 0 or rerank_options.max_doc_chars == 0)) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const indexed_embeddings = try search_sqlite.countSearchEmbeddings(db, table_id.?);
    const semantic_enabled = embedding_requested and indexed_embeddings > 0;
    const retrieval_limit = if (rerank_options.enabled)
        @max(limit, rerank_options.candidate_limit)
    else
        limit;

    const bm25_matches = try search_sqlite.searchFts5Bm25(db, allocator, table_id.?, query.?, retrieval_limit);
    defer allocator.free(bm25_matches);

    const empty_vector_matches: []interfaces.VectorSearchMatch = &.{};
    var owned_vector_matches: ?[]interfaces.VectorSearchMatch = null;
    defer if (owned_vector_matches) |matches| allocator.free(matches);

    if (semantic_enabled) {
        var store = try search_sqlite.loadSearchStore(db, allocator);
        defer store.deinit();

        const query_embedding = try embedContextualText(allocator, query.?, .{
            .enabled = true,
            .embedding_base_url = base_url,
            .embedding_api_key = api_key,
            .embedding_model = embedding_model,
        });
        defer allocator.free(query_embedding);

        const vector_repo = store.vectorRepository();
        owned_vector_matches = try vector_repo.searchNearestFn(vector_repo.ctx, allocator, .{
            .table_name = "search_embeddings",
            .key_column = "doc_id",
            .vector_column = "embedding_blob",
            .query_vector = query_embedding,
            .limit = retrieval_limit,
            .table_id = table_id,
        });
    }

    const vector_matches = owned_vector_matches orelse empty_vector_matches;

    const fused_matches = try fuseBm25AndVectorMatches(allocator, bm25_matches, vector_matches, vector_weight, retrieval_limit);
    defer allocator.free(fused_matches);

    var matches = try contextualMatchesFromHybrid(allocator, fused_matches);
    defer allocator.free(matches);

    if (rerank_options.enabled) {
        try applyLlmRerank(allocator, db, table_id.?, query.?, &matches, rerank_options);
    }

    if (matches.len > limit) matches.len = limit;

    try writeStdout("{f}\n", .{std.json.fmt(.{
        .table_id = table_id.?,
        .query = query.?,
        .retrieval_mode = if (semantic_enabled) "hybrid_bm25_vector" else "bm25",
        .semantic_status = if (!embedding_requested) "not_requested" else if (indexed_embeddings == 0) "no_indexed_embeddings" else "ok",
        .indexed_embeddings = indexed_embeddings,
        .vector_weight = vector_weight,
        .rerank_enabled = rerank_options.enabled,
        .matches = matches,
    }, .{})});
}

fn runSearchEmbeddingBatchCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var table_id: ?u64 = null;
    var embedding_base_url: ?[]const u8 = null;
    var embedding_api_key: ?[]const u8 = null;
    var embedding_model: ?[]const u8 = null;
    var limit: usize = 100;
    var missing_only = false;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) {
            db_path = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--table-id")) {
            table_id = std.fmt.parseInt(u64, try requireArg(args, &index), 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--embedding-base-url") or std.mem.eql(u8, arg, "--base-url")) {
            embedding_base_url = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--embedding-api-key") or std.mem.eql(u8, arg, "--api-key")) {
            embedding_api_key = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--embedding-model")) {
            embedding_model = try requireArg(args, &index);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            limit = std.fmt.parseInt(usize, try requireArg(args, &index), 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--missing-only")) {
            missing_only = true;
        } else return CliError.InvalidArguments;
    }

    if (db_path == null or table_id == null or embedding_base_url == null or embedding_model == null or limit == 0) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    const rows = try search_sqlite.loadSearchDocumentsForEmbeddingBatch(db, allocator, table_id.?, limit, missing_only);
    defer {
        for (rows) |row| row.deinit(allocator);
        allocator.free(rows);
    }

    if (rows.len == 0) {
        try writeStdout("{f}\n", .{std.json.fmt(.{
            .table_id = table_id.?,
            .processed = 0,
            .missing_only = missing_only,
        }, .{})});
        return;
    }

    const inputs = try allocator.alloc([]const u8, rows.len);
    defer allocator.free(inputs);
    for (rows, 0..) |row, i| inputs[i] = row.content;

    const provider = llm.ProviderConfig{
        .name = "search-embedding-batch",
        .kind = .openai_compatible,
        .base_url = embedding_base_url.?,
        .api_key = embedding_api_key,
        .model = embedding_model.?,
        .capabilities = &.{.embeddings},
    };
    const manager = llm.Manager.init(.{
        .providers = &.{provider},
        .default_provider = "search-embedding-batch",
    });

    var response = try manager.embedTexts(allocator, mindbrain.zig16_compat.io(), null, inputs);
    defer response.deinit(allocator);
    if (response.vectors.len != rows.len) return error.InvalidResponse;

    for (rows, response.vectors) |row, vector| {
        try search_sqlite.upsertSearchEmbedding(db, allocator, row.table_id, row.doc_id, vector.values);
    }

    try writeStdout("{f}\n", .{std.json.fmt(.{
        .table_id = table_id.?,
        .processed = rows.len,
        .missing_only = missing_only,
    }, .{})});
}

const FusionScore = struct {
    bm25_score: f64 = 0.0,
    vector_score: f64 = 0.0,
};

const ContextualSearchMatch = struct {
    doc_id: interfaces.DocId,
    bm25_score: f64,
    vector_score: f64,
    combined_score: f64,
    rerank_score: ?f64 = null,
};

const RerankOptions = struct {
    enabled: bool = false,
    provider_kind: llm.ProviderKind = .openai_compatible,
    base_url: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
    model: ?[]const u8 = null,
    candidate_limit: usize = 50,
    max_doc_chars: usize = 1200,
};

const RerankScoreRow = struct {
    doc_id: u64,
    score: f64,
};

const RerankResponse = struct {
    scores: []RerankScoreRow,
};

fn fuseBm25AndVectorMatches(
    allocator: Allocator,
    bm25_matches: []const search_sqlite.Bm25Match,
    vector_matches: []const interfaces.VectorSearchMatch,
    vector_weight: f64,
    limit: usize,
) ![]interfaces.HybridSearchMatch {
    var scores = std.AutoHashMap(interfaces.DocId, FusionScore).init(allocator);
    defer scores.deinit();

    for (bm25_matches) |match| {
        const entry = try getOrPutFusionScore(&scores, match.doc_id);
        entry.bm25_score = match.score;
    }

    for (vector_matches) |match| {
        const entry = try getOrPutFusionScore(&scores, match.doc_id);
        if (match.similarity > entry.vector_score) entry.vector_score = match.similarity;
    }

    var results = std.ArrayList(interfaces.HybridSearchMatch).empty;
    defer results.deinit(allocator);

    var it = scores.iterator();
    while (it.next()) |entry| {
        const bm25_score = if (std.math.isFinite(entry.value_ptr.bm25_score)) entry.value_ptr.bm25_score else 0.0;
        const vector_score = if (std.math.isFinite(entry.value_ptr.vector_score)) entry.value_ptr.vector_score else 0.0;
        var combined_score = bm25_score * (1.0 - vector_weight) + vector_score * vector_weight;
        if (!std.math.isFinite(combined_score)) combined_score = 0.0;
        try results.append(allocator, .{
            .doc_id = entry.key_ptr.*,
            .bm25_score = bm25_score,
            .vector_score = vector_score,
            .combined_score = combined_score,
        });
    }

    std.mem.sort(interfaces.HybridSearchMatch, results.items, {}, struct {
        fn lessThan(_: void, a: interfaces.HybridSearchMatch, b: interfaces.HybridSearchMatch) bool {
            return a.combined_score > b.combined_score;
        }
    }.lessThan);

    if (results.items.len > limit) results.shrinkRetainingCapacity(limit);
    return results.toOwnedSlice(allocator);
}

fn contextualMatchesFromHybrid(
    allocator: Allocator,
    matches: []const interfaces.HybridSearchMatch,
) ![]ContextualSearchMatch {
    const out = try allocator.alloc(ContextualSearchMatch, matches.len);
    for (matches, 0..) |match, i| {
        out[i] = .{
            .doc_id = match.doc_id,
            .bm25_score = match.bm25_score,
            .vector_score = match.vector_score,
            .combined_score = match.combined_score,
        };
    }
    return out;
}

fn applyLlmRerank(
    allocator: Allocator,
    db: facet_sqlite.Database,
    table_id: u64,
    query: []const u8,
    matches: *[]ContextualSearchMatch,
    options: RerankOptions,
) !void {
    if (matches.len == 0) return;
    const prompt = try buildRerankPrompt(allocator, db, table_id, query, matches.*, options.max_doc_chars);
    defer allocator.free(prompt);

    const provider = llm.ProviderConfig{
        .name = "search-reranker",
        .kind = options.provider_kind,
        .base_url = options.base_url.?,
        .api_key = options.api_key,
        .model = options.model.?,
        .capabilities = &.{ .chat, .json_output },
    };
    const manager = llm.Manager.init(.{
        .providers = &.{provider},
        .default_provider = "search-reranker",
    });
    const messages = [_]llm.Message{
        .{
            .role = "system",
            .content = "You rerank search candidates. Return only JSON with a scores array. Each score must use an input doc_id and a relevance score from 0 to 1.",
        },
        .{ .role = "user", .content = prompt },
    };

    var response = try manager.chat(allocator, mindbrain.zig16_compat.io(), &messages, .{
        .json_mode = true,
        .temperature = 0.0,
        .max_tokens = 1200,
    });
    defer response.deinit(allocator);

    var parsed = try std.json.parseFromSlice(RerankResponse, allocator, response.content, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const valid_scores = applyRerankScores(matches.*, parsed.value.scores);
    if (valid_scores == 0) return error.InvalidResponse;

    sortContextualMatchesByRerank(matches.*);
}

fn applyRerankScores(matches: []ContextualSearchMatch, scores: []const RerankScoreRow) usize {
    var valid_scores: usize = 0;
    for (scores) |score| {
        if (!std.math.isFinite(score.score)) continue;
        for (matches) |*match| {
            if (match.doc_id == score.doc_id) {
                match.rerank_score = @max(0.0, @min(1.0, score.score));
                valid_scores += 1;
                break;
            }
        }
    }
    return valid_scores;
}

fn sortContextualMatchesByRerank(matches: []ContextualSearchMatch) void {
    std.mem.sort(ContextualSearchMatch, matches, {}, struct {
        fn lessThan(_: void, a: ContextualSearchMatch, b: ContextualSearchMatch) bool {
            const ar = a.rerank_score orelse 0.0;
            const br = b.rerank_score orelse 0.0;
            if (ar == br) return a.combined_score > b.combined_score;
            return ar > br;
        }
    }.lessThan);
}

fn buildRerankPrompt(
    allocator: Allocator,
    db: facet_sqlite.Database,
    table_id: u64,
    query: []const u8,
    matches: []const ContextualSearchMatch,
    max_doc_chars: usize,
) ![]u8 {
    var out = std.Io.Writer.Allocating.init(allocator);
    errdefer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("Query: ");
    try writer.print("{f}", .{std.json.fmt(query, .{})});
    try writer.writeAll("\nCandidates JSON lines:\n");

    for (matches) |match| {
        const content = search_sqlite.loadSearchDocumentContent(db, allocator, table_id, match.doc_id) catch |err| switch (err) {
            error.MissingRow => continue,
            else => return err,
        };
        defer allocator.free(content);
        const clipped = content[0..@min(content.len, max_doc_chars)];
        try writer.print("{f}\n", .{std.json.fmt(.{
            .doc_id = match.doc_id,
            .bm25_score = match.bm25_score,
            .vector_score = match.vector_score,
            .combined_score = match.combined_score,
            .content = clipped,
        }, .{})});
    }

    try writer.writeAll("Return JSON: {\"scores\":[{\"doc_id\":<number>,\"score\":<0..1>}]}");
    return try out.toOwnedSlice();
}

fn getOrPutFusionScore(
    scores: *std.AutoHashMap(interfaces.DocId, FusionScore),
    doc_id: interfaces.DocId,
) !*FusionScore {
    const gop = try scores.getOrPut(doc_id);
    if (!gop.found_existing) gop.value_ptr.* = .{};
    return gop.value_ptr;
}

fn parseProviderKind(value: []const u8) ?llm.ProviderKind {
    if (std.mem.eql(u8, value, "openai_compatible")) return .openai_compatible;
    if (std.mem.eql(u8, value, "openai")) return .openai;
    if (std.mem.eql(u8, value, "openrouter")) return .openrouter;
    if (std.mem.eql(u8, value, "ollama")) return .ollama;
    if (std.mem.eql(u8, value, "vllm")) return .vllm;
    if (std.mem.eql(u8, value, "llama_cpp")) return .llama_cpp;
    if (std.mem.eql(u8, value, "deepseek")) return .deepseek;
    if (std.mem.eql(u8, value, "gemini")) return .gemini;
    if (std.mem.eql(u8, value, "anthropic")) return .anthropic;
    return null;
}

test "rerank scores clamp and reorder contextual matches" {
    var matches = [_]ContextualSearchMatch{
        .{ .doc_id = 1, .bm25_score = 2.0, .vector_score = 0.2, .combined_score = 1.1 },
        .{ .doc_id = 2, .bm25_score = 1.0, .vector_score = 0.9, .combined_score = 1.0 },
        .{ .doc_id = 3, .bm25_score = 0.8, .vector_score = 0.1, .combined_score = 0.6 },
    };
    const scores = [_]RerankScoreRow{
        .{ .doc_id = 2, .score = 1.5 },
        .{ .doc_id = 3, .score = -0.5 },
        .{ .doc_id = 99, .score = 0.9 },
    };

    try std.testing.expectEqual(@as(usize, 2), applyRerankScores(&matches, &scores));
    sortContextualMatchesByRerank(&matches);

    try std.testing.expectEqual(@as(u64, 2), matches[0].doc_id);
    try std.testing.expectEqual(@as(f64, 1.0), matches[0].rerank_score.?);
    try std.testing.expectEqual(@as(u64, 1), matches[1].doc_id);
    try std.testing.expect(matches[1].rerank_score == null);
    try std.testing.expectEqual(@as(u64, 3), matches[2].doc_id);
    try std.testing.expectEqual(@as(f64, 0.0), matches[2].rerank_score.?);
}

fn profileMetadataJson(
    allocator: Allocator,
    profile_value: std.json.Value,
    decision: chunking_policy.ChunkingDecision,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, .{
        .pipeline = "document_profile_worker",
        .document_profile = profile_value,
        .chunking_decision = .{
            .splitter_profile = @tagName(decision.profile),
            .requires_specialized_splitter = decision.requires_specialized_splitter,
            .rationale = decision.rationale,
            .options = .{
                .strategy = decision.options.strategy.label(),
                .target_tokens = decision.options.target_tokens,
                .overlap_tokens = decision.options.overlap_tokens,
                .max_chars = decision.options.max_chars,
                .min_chars = decision.options.min_chars,
            },
        },
    }, .{});
}

fn chunkingMetadataJson(
    allocator: Allocator,
    decision: chunking_policy.ChunkingDecision,
    contextual: ?ChunkContextualMetadata,
) ![]u8 {
    if (contextual) |ctx| {
        return try std.json.Stringify.valueAlloc(allocator, .{
            .pipeline = "document_profile_worker",
            .splitter_profile = @tagName(decision.profile),
            .rationale = decision.rationale,
            .contextual_retrieval = .{
                .enabled = true,
                .method = "llm_chunk_context",
                .context = ctx.context,
                .contextualized_content = ctx.contextualized_content,
                .doc_context_chars = ctx.doc_context_chars,
            },
        }, .{});
    }
    return try std.json.Stringify.valueAlloc(allocator, .{
        .pipeline = "document_profile_worker",
        .splitter_profile = @tagName(decision.profile),
        .rationale = decision.rationale,
        .contextual_retrieval = .{ .enabled = false },
    }, .{});
}

fn contextualizeChunk(
    allocator: Allocator,
    document: []const u8,
    source_ref: []const u8,
    profile: corpus_profile.DocumentProfile,
    decision: chunking_policy.ChunkingDecision,
    ch: chunker.Chunk,
    options: ContextualRetrievalOptions,
) !ChunkContextualMetadata {
    const doc_context = try boundedDocumentContext(allocator, document, options.max_doc_chars);
    defer allocator.free(doc_context);

    const provider = llm.ProviderConfig{
        .name = "default",
        .kind = .openai_compatible,
        .base_url = options.base_url orelse return CliError.InvalidArguments,
        .api_key = options.api_key,
        .model = options.model orelse return CliError.InvalidArguments,
    };
    const manager = llm.Manager.init(.{
        .providers = &.{provider},
        .default_provider = "default",
    });

    const system =
        \\You generate retrieval context for document chunks.
        \\Return only one concise sentence or short paragraph.
        \\Do not summarize the whole document. Explain what this chunk is about
        \\and how it fits in the document so retrieval can match specific queries.
        \\Do not invent facts beyond the document and chunk.
    ;
    const user = try std.fmt.allocPrint(allocator,
        \\<document_context source="{s}" kind="{s}" language="{s}" splitter="{s}">
        \\{s}
        \\</document_context>
        \\
        \\<chunk index="{d}" strategy="{s}">
        \\{s}
        \\</chunk>
        \\
        \\Write 50-100 tokens of context to prepend before this chunk for contextual embeddings and contextual BM25.
    , .{
        source_ref,
        @tagName(profile.document_kind),
        profile.language,
        @tagName(decision.profile),
        doc_context,
        ch.index,
        ch.strategy,
        ch.content,
    });
    defer allocator.free(user);

    const messages = [_]llm.Message{
        .{ .role = "system", .content = system },
        .{ .role = "user", .content = user },
    };
    var response = try manager.chat(allocator, mindbrain.zig16_compat.io(), &messages, .{
        .temperature = options.temperature,
        .max_tokens = options.max_tokens,
    });
    defer response.deinit(allocator);

    const context = try trimAndDupe(allocator, response.content);
    errdefer allocator.free(context);
    const contextualized_content = try std.fmt.allocPrint(allocator, "{s}\n\n{s}", .{ context, ch.content });
    return .{
        .context = context,
        .contextualized_content = contextualized_content,
        .doc_context_chars = doc_context.len,
    };
}

fn boundedDocumentContext(allocator: Allocator, document: []const u8, max_chars: usize) ![]u8 {
    if (max_chars == 0 or document.len <= max_chars) return try allocator.dupe(u8, document);
    if (max_chars < 64) return try allocator.dupe(u8, document[0..max_chars]);
    const prefix_len = max_chars / 2;
    const suffix_len = max_chars - prefix_len;
    return try std.fmt.allocPrint(
        allocator,
        "{s}\n\n[...document truncated for contextual retrieval...]\n\n{s}",
        .{ document[0..prefix_len], document[document.len - suffix_len ..] },
    );
}

fn trimAndDupe(allocator: Allocator, text: []const u8) ![]u8 {
    return try allocator.dupe(u8, std.mem.trim(u8, text, " \t\r\n\""));
}

fn runDocumentProfileDirectory(allocator: Allocator, opts: DocumentProfileDirOptions) !void {
    var files = std.ArrayList([]u8).empty;
    defer {
        for (files.items) |path| allocator.free(path);
        files.deinit(allocator);
    }
    try collectRegularFilesRecursive(allocator, opts.dir_path, &files);
    std.mem.sort([]u8, files.items, {}, stringSliceLessThan);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeByte('[');
    for (files.items, 0..) |path, i| {
        if (i > 0) try out.writer.writeByte(',');
        const text = std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), path, allocator, .unlimited) catch |err| {
            try out.writer.print("{f}", .{std.json.fmt(.{
                .source_ref = path,
                .ok = false,
                .@"error" = @errorName(err),
            }, .{})});
            continue;
        };
        defer allocator.free(text);

        const source = if (opts.source_ref) |base|
            try std.fs.path.join(allocator, &.{ base, path })
        else
            try allocator.dupe(u8, path);
        defer allocator.free(source);

        const profile_json = profileDocumentContent(allocator, .{
            .text = text,
            .source_ref = source,
            .llm_provider_arg = opts.llm_provider_arg,
            .base_url = opts.base_url,
            .api_key = opts.api_key,
            .model = opts.model,
            .env_values = opts.env_values,
            .sample_chars = opts.sample_chars,
            .temperature = opts.temperature,
            .max_tokens = opts.max_tokens,
            .dry_run = opts.dry_run,
            .mock_profile_file = opts.mock_profile_file,
        }) catch |err| {
            try out.writer.print("{f}", .{std.json.fmt(.{
                .source_ref = source,
                .ok = false,
                .@"error" = @errorName(err),
            }, .{})});
            continue;
        };
        defer allocator.free(profile_json);

        if (opts.dry_run) {
            try out.writer.writeAll(profile_json);
        } else {
            var parsed = try std.json.parseFromSlice(std.json.Value, allocator, profile_json, .{});
            defer parsed.deinit();
            try out.writer.print("{f}", .{std.json.fmt(.{
                .source_ref = source,
                .ok = true,
                .profile = parsed.value,
            }, .{})});
        }
    }
    try out.writer.writeByte(']');
    const body = try out.toOwnedSlice();
    defer allocator.free(body);
    try writeStdout("{s}\n", .{body});
}

fn collectRegularFilesRecursive(
    allocator: Allocator,
    dir_path: []const u8,
    files: *std.ArrayList([]u8),
) !void {
    var dir = try std.Io.Dir.cwd().openDir(mindbrain.zig16_compat.io(), dir_path, .{ .iterate = true });
    defer dir.close(mindbrain.zig16_compat.io());

    var iter = dir.iterateAssumeFirstIteration();
    while (try iter.next(mindbrain.zig16_compat.io())) |entry| {
        const child = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
        errdefer allocator.free(child);
        switch (entry.kind) {
            .file => try files.append(allocator, child),
            .directory => {
                try collectRegularFilesRecursive(allocator, child, files);
                allocator.free(child);
            },
            else => allocator.free(child),
        }
    }
}

fn stringSliceLessThan(_: void, a: []u8, b: []u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn runDocumentIngestCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var collection_id: ?[]const u8 = null;
    var doc_id_opt: ?u64 = null;
    var nanoid_value: []const u8 = "";
    var source_ref: ?[]const u8 = null;
    var ingested_at: ?[]const u8 = null;
    var language: []const u8 = "english";
    var ontology_id: ?[]const u8 = null;
    var content: ?[]const u8 = null;
    var content_file: ?[]const u8 = null;
    var options = chunker.Options{};

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--collection-id")) collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--doc-id")) {
            const v = try requireArg(args, &index);
            doc_id_opt = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--nanoid")) nanoid_value = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--source-ref")) source_ref = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ingested-at")) ingested_at = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--language")) language = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--ontology-id")) ontology_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--content")) content = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--content-file")) content_file = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--strategy")) {
            const v = try requireArg(args, &index);
            options.strategy = parseChunkerStrategy(v) orelse return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--target-tokens")) {
            const v = try requireArg(args, &index);
            options.target_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--overlap-tokens")) {
            const v = try requireArg(args, &index);
            options.overlap_tokens = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-chars")) {
            const v = try requireArg(args, &index);
            options.max_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--min-chars")) {
            const v = try requireArg(args, &index);
            options.min_chars = std.fmt.parseInt(usize, v, 10) catch return CliError.InvalidArguments;
        } else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or collection_id == null or doc_id_opt == null) {
        return CliError.InvalidArguments;
    }
    if ((content == null) == (content_file == null)) {
        // Exactly one of --content / --content-file must be provided.
        return CliError.InvalidArguments;
    }

    var loaded_content: ?[]u8 = null;
    defer if (loaded_content) |buf| allocator.free(buf);
    const text: []const u8 = if (content) |c| c else blk: {
        const buf = try std.Io.Dir.cwd().readFileAlloc(mindbrain.zig16_compat.io(), content_file.?, allocator, .unlimited);
        loaded_content = buf;
        break :blk buf;
    };

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    // Resolve / generate the public nanoid up front so we can echo it back.
    var generated_nanoid: ?[]u8 = null;
    defer if (generated_nanoid) |b| allocator.free(b);
    const nanoid_final: []const u8 = if (nanoid_value.len == 0) blk: {
        generated_nanoid = try nanoid.generateDefault(allocator);
        break :blk generated_nanoid.?;
    } else nanoid_value;

    try collections_sqlite.upsertDocumentRaw(db, .{
        .workspace_id = workspace_id.?,
        .collection_id = collection_id.?,
        .doc_id = doc_id_opt.?,
        .doc_nanoid = nanoid_final,
        .content = text,
        .language = language,
        .source_ref = source_ref,
    });

    var owned_ontology: ?[]const u8 = null;
    defer if (owned_ontology) |o| allocator.free(o);
    const ontology_final: []const u8 = if (ontology_id) |o| o else blk: {
        owned_ontology = try collections_sqlite.ensureDefaultOntology(db, allocator, workspace_id.?);
        break :blk owned_ontology.?;
    };

    const chunks = try chunker.chunk(allocator, text, options);
    defer chunker.freeChunks(allocator, chunks);
    const total_chunks: u32 = @intCast(chunks.len);

    for (chunks) |ch| {
        try collections_sqlite.upsertChunkRaw(db, .{
            .workspace_id = workspace_id.?,
            .collection_id = collection_id.?,
            .doc_id = doc_id_opt.?,
            .chunk_index = ch.index,
            .content = ch.content,
            .language = language,
            .offset_start = ch.offset_start,
            .offset_end = ch.offset_end,
            .strategy = ch.strategy,
            .token_count = ch.token_count,
            .parent_chunk_index = ch.parent_chunk_index,
        });

        var source_facets = try chunker.deriveSourceFacets(allocator, .{
            .workspace_id = workspace_id.?,
            .collection_id = collection_id.?,
            .ontology_id = ontology_final,
            .doc_id = doc_id_opt.?,
            .ingested_at = ingested_at,
            .source_ref = source_ref,
        }, ch, total_chunks);
        defer source_facets.deinit();
        for (source_facets.rows) |row| {
            try collections_sqlite.upsertFacetAssignmentRaw(db, row);
        }
    }

    try writeStdout("ingested doc_id={d} nanoid={s} chunks={d}\n", .{
        doc_id_opt.?, nanoid_final, total_chunks,
    });
}

fn runDocumentByNanoidCommand(allocator: Allocator, args: []const []const u8) !void {
    var db_path: ?[]const u8 = null;
    var nanoid_value: ?[]const u8 = null;

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--nanoid")) nanoid_value = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or nanoid_value == null) return CliError.InvalidArguments;

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();

    const lookup = try collections_sqlite.lookupDocByNanoid(db, allocator, nanoid_value.?);
    if (lookup) |found| {
        defer found.deinit(allocator);
        try writeStdout("workspace_id={s} collection_id={s} doc_id={d}\n", .{
            found.workspace_id, found.collection_id, found.doc_id,
        });
    } else {
        try writeStdout("not found\n", .{});
    }
}

fn runExternalLinkAddCommand(allocator: Allocator, args: []const []const u8) !void {
    _ = allocator;
    var db_path: ?[]const u8 = null;
    var workspace_id: ?[]const u8 = null;
    var source_collection_id: ?[]const u8 = null;
    var source_doc_id_opt: ?u64 = null;
    var source_chunk_index: ?u32 = null;
    var target_uri: ?[]const u8 = null;
    var edge_type: []const u8 = "external_link";
    var weight: f64 = 1.0;
    var link_id: u64 = 0;
    var metadata_json: []const u8 = "{}";

    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--db")) db_path = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--workspace-id")) workspace_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--source-collection-id")) source_collection_id = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--source-doc-id")) {
            const v = try requireArg(args, &index);
            source_doc_id_opt = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--source-chunk-index")) {
            const v = try requireArg(args, &index);
            source_chunk_index = std.fmt.parseInt(u32, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--target-uri")) target_uri = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--edge-type")) edge_type = try requireArg(args, &index) else if (std.mem.eql(u8, arg, "--weight")) {
            const v = try requireArg(args, &index);
            weight = std.fmt.parseFloat(f64, v) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--link-id")) {
            const v = try requireArg(args, &index);
            link_id = std.fmt.parseInt(u64, v, 10) catch return CliError.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--metadata-json")) metadata_json = try requireArg(args, &index) else return CliError.InvalidArguments;
    }
    if (db_path == null or workspace_id == null or source_collection_id == null or source_doc_id_opt == null or target_uri == null) {
        return CliError.InvalidArguments;
    }

    var db = try facet_sqlite.Database.open(db_path.?);
    defer db.close();
    try db.applyStandaloneSchema();

    // When --link-id is omitted (or 0), allocate the next free id within the
    // workspace so the caller does not have to track them manually.
    if (link_id == 0) {
        const sql = "SELECT COALESCE(MAX(link_id), 0) + 1 FROM external_links_raw WHERE workspace_id = ?1";
        const stmt = try facet_sqlite.prepare(db, sql);
        defer facet_sqlite.finalize(stmt);
        try facet_sqlite.bindText(stmt, 1, workspace_id.?);
        if (facet_sqlite.c.sqlite3_step(stmt) != facet_sqlite.c.SQLITE_ROW) return error.StepFailed;
        link_id = @intCast(facet_sqlite.c.sqlite3_column_int64(stmt, 0));
    }

    try collections_sqlite.upsertExternalLinkRaw(db, .{
        .workspace_id = workspace_id.?,
        .link_id = link_id,
        .source_collection_id = source_collection_id.?,
        .source_doc_id = source_doc_id_opt.?,
        .source_chunk_index = source_chunk_index,
        .target_uri = target_uri.?,
        .edge_type = edge_type,
        .weight = weight,
        .metadata_json = metadata_json,
    });

    try writeStdout("external link {d} -> {s} ({s}) added\n", .{ link_id, target_uri.?, edge_type });
}

fn writeStdout(comptime fmt: []const u8, args: anytype) !void {
    var stdout_file_writer = std.Io.File.stdout().writer(mindbrain.zig16_compat.io(), &.{});
    const stdout = &stdout_file_writer.interface;
    try stdout.print(fmt, args);
    try stdout.flush();
}
