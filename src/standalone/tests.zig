const std = @import("std");

// Tests are only discovered for files that belong to the test root's *module*.
// Listing each standalone source via direct relative @import keeps them inside
// this same module so their `test ""` blocks are picked up by the test runner.
// (Refs through cross-module @import("mindbrain") would create a separate
// module whose tests would be invisible to this runner.)
comptime {
    @setEvalBranchQuota(64_000);
    _ = @import("collections_io.zig");
    _ = @import("collections_sqlite.zig");
    _ = @import("compatibility_sqlite.zig");
    _ = @import("corpus_eval.zig");
    _ = @import("corpus_profile.zig");
    _ = @import("corpus_profile_prompt.zig");
    _ = @import("data_sources_test.zig");
    _ = @import("db_benchmark.zig");
    _ = @import("document_normalize.zig");
    _ = @import("facet_parity_test.zig");
    _ = @import("bm25_stopwords_sqlite.zig");
    _ = @import("facet_sqlite.zig");
    _ = @import("facts_sqlite.zig");
    _ = @import("facet_store.zig");
    _ = @import("fixture_loader.zig");
    _ = @import("fixture_repositories.zig");
    _ = @import("graph_sqlite.zig");
    _ = @import("graph_store.zig");
    _ = @import("helper_api.zig");
    _ = @import("http_server_config.zig");
    _ = @import("hybrid_search.zig");
    _ = @import("import_pipeline.zig");
    _ = @import("legal_chunker.zig");
    _ = @import("llm.zig");
    _ = @import("llm/gemini/client.zig");
    _ = @import("llm/openai_compat/responses.zig");
    _ = @import("llm_client.zig");
    _ = @import("nanoid.zig");
    _ = @import("chunker.zig");
    _ = @import("chunking_policy.zig");
    _ = @import("native_compat_test.zig");
    _ = @import("ontology_sqlite.zig");
    _ = @import("pragma_dsl.zig");
    _ = @import("pragma_sqlite.zig");
    _ = @import("query_executor.zig");
    _ = @import("queue_sqlite.zig");
    _ = @import("reference_extractor.zig");
    _ = @import("search_compact_store.zig");
    _ = @import("search_sqlite.zig");
    _ = @import("search_store.zig");
    _ = @import("sqlite_schema.zig");
    _ = @import("tokenization_sqlite.zig");
    _ = @import("toon_exports.zig");
    _ = @import("vector_blob.zig");
    _ = @import("vector_distance.zig");
    _ = @import("vector_sqlite_exact.zig");
    _ = @import("workspace_sqlite.zig");
}
