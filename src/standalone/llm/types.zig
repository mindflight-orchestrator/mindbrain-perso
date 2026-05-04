const std = @import("std");

pub const ProviderKind = enum {
    openai_compatible,
    openai,
    openrouter,
    ollama,
    vllm,
    llama_cpp,
    deepseek,
    gemini,
    anthropic,
};

pub const Capability = enum {
    chat,
    responses,
    json_output,
    tool_use,
    vision,
    audio,
    streaming,
    embeddings,
    reasoning,
    caching,
};

pub const ProviderConfig = struct {
    name: []const u8,
    kind: ProviderKind = .openai_compatible,
    base_url: []const u8,
    api_key: ?[]const u8 = null,
    model: []const u8,
    embedding_model: ?[]const u8 = null,
    audio_model: ?[]const u8 = null,
    capabilities: []const Capability = &.{ .chat, .json_output },
};

pub const ManagerConfig = struct {
    providers: []const ProviderConfig,
    default_provider: []const u8,
};

pub const ImageUrl = struct {
    url: []const u8,
    detail: ?[]const u8 = null,
};

pub const InlineData = struct {
    mime_type: []const u8,
    data: []const u8,
    detail: ?[]const u8 = null,
};

pub const ContentPart = union(enum) {
    text: []const u8,
    image_url: ImageUrl,
    image_base64: InlineData,
    audio_base64: InlineData,
    file_base64: InlineData,
};

pub const Message = struct {
    role: []const u8,
    content: []const u8 = "",
    parts: []const ContentPart = &.{},
};

pub const Tool = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    parameters_json: []const u8 = "{}",
};

pub const ToolChoice = union(enum) {
    auto,
    none,
    required,
    named: []const u8,
};

pub const ChatRequest = struct {
    messages: []const Message,
    temperature: ?f32 = null,
    max_tokens: ?u32 = null,
    json_mode: bool = false,
    tools: []const Tool = &.{},
    tool_choice: ?ToolChoice = null,
};

pub const ChatOptions = struct {
    provider: ?[]const u8 = null,
    temperature: ?f32 = null,
    max_tokens: ?u32 = null,
    json_mode: bool = false,
    tools: []const Tool = &.{},
    tool_choice: ?ToolChoice = null,
};

pub const ToolCall = struct {
    id: []u8,
    name: []u8,
    arguments_json: []u8,

    pub fn deinit(self: ToolCall, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.name);
        allocator.free(self.arguments_json);
    }
};

pub const ChatResponse = struct {
    content: []u8,
    raw_json: []u8,
    tool_calls: []ToolCall = &.{},

    pub fn deinit(self: ChatResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.content);
        allocator.free(self.raw_json);
        for (self.tool_calls) |call| call.deinit(allocator);
        if (self.tool_calls.len > 0) allocator.free(self.tool_calls);
    }
};

pub const ResponseFunctionCallOutput = struct {
    call_id: []const u8,
    output: []const u8,
};

pub const ResponseInputItem = union(enum) {
    message: Message,
    function_call_output: ResponseFunctionCallOutput,
};

pub const ResponseInput = union(enum) {
    text: []const u8,
    items: []const ResponseInputItem,
};

pub const ResponseRequest = struct {
    input: ResponseInput,
    instructions: ?[]const u8 = null,
    temperature: ?f32 = null,
    max_output_tokens: ?u32 = null,
    store: ?bool = null,
    previous_response_id: ?[]const u8 = null,
    text_format_json: ?[]const u8 = null,
    tools: []const Tool = &.{},
    tool_choice: ?ToolChoice = null,
    stream: bool = false,
};

pub const ResponseOptions = struct {
    provider: ?[]const u8 = null,
};

pub const ResponseMessageOutput = struct {
    id: []u8,
    role: []u8,
    text: []u8,

    pub fn deinit(self: ResponseMessageOutput, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.role);
        allocator.free(self.text);
    }
};

pub const ResponseFunctionCall = struct {
    id: []u8,
    call_id: []u8,
    name: []u8,
    arguments_json: []u8,

    pub fn deinit(self: ResponseFunctionCall, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.call_id);
        allocator.free(self.name);
        allocator.free(self.arguments_json);
    }
};

pub const ResponseFunctionCallOutputOwned = struct {
    id: []u8,
    call_id: []u8,
    output: []u8,

    pub fn deinit(self: ResponseFunctionCallOutputOwned, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.call_id);
        allocator.free(self.output);
    }
};

pub const ResponseReasoningOutput = struct {
    id: []u8,
    summary: []u8,

    pub fn deinit(self: ResponseReasoningOutput, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.summary);
    }
};

pub const ResponseUnknownOutput = struct {
    item_type: []u8,
    raw_json: []u8,

    pub fn deinit(self: ResponseUnknownOutput, allocator: std.mem.Allocator) void {
        allocator.free(self.item_type);
        allocator.free(self.raw_json);
    }
};

pub const ResponseOutputItem = union(enum) {
    message: ResponseMessageOutput,
    output_text: []u8,
    function_call: ResponseFunctionCall,
    function_call_output: ResponseFunctionCallOutputOwned,
    reasoning: ResponseReasoningOutput,
    unknown: ResponseUnknownOutput,

    pub fn deinit(self: ResponseOutputItem, allocator: std.mem.Allocator) void {
        switch (self) {
            .message => |item| item.deinit(allocator),
            .output_text => |text| allocator.free(text),
            .function_call => |item| item.deinit(allocator),
            .function_call_output => |item| item.deinit(allocator),
            .reasoning => |item| item.deinit(allocator),
            .unknown => |item| item.deinit(allocator),
        }
    }
};

pub const ResponseResult = struct {
    id: []u8,
    status: []u8,
    output_text: []u8,
    output_items: []ResponseOutputItem,
    raw_json: []u8,

    pub fn deinit(self: ResponseResult, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.status);
        allocator.free(self.output_text);
        for (self.output_items) |item| item.deinit(allocator);
        allocator.free(self.output_items);
        allocator.free(self.raw_json);
    }
};

pub const StreamEventKind = enum {
    text_delta,
    tool_call_delta,
    response_created,
    output_item_added,
    output_text_delta,
    function_call_arguments_delta,
    completed,
    usage,
    done,
    error_event,
};

pub const StreamEvent = struct {
    kind: StreamEventKind,
    text: ?[]const u8 = null,
    tool_id: ?[]const u8 = null,
    tool_name: ?[]const u8 = null,
    arguments_delta: ?[]const u8 = null,
    raw_json: ?[]const u8 = null,
};

pub const EmbeddingRequest = struct {
    model: []const u8,
    input: []const []const u8,
};

pub const EmbeddingVector = struct {
    values: []f32,
};

pub const EmbeddingResponse = struct {
    vectors: []EmbeddingVector,
    raw_json: []u8,

    pub fn deinit(self: EmbeddingResponse, allocator: std.mem.Allocator) void {
        for (self.vectors) |vector| allocator.free(vector.values);
        allocator.free(self.vectors);
        allocator.free(self.raw_json);
    }
};

pub const AudioTranscriptionRequest = struct {
    model: []const u8,
    filename: []const u8,
    mime_type: []const u8 = "application/octet-stream",
    audio_bytes: []const u8,
    language: ?[]const u8 = null,
    prompt: ?[]const u8 = null,
    response_format: ?[]const u8 = null,
};

pub const AudioTranscriptionResponse = struct {
    text: []u8,
    raw_json: []u8,

    pub fn deinit(self: AudioTranscriptionResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        allocator.free(self.raw_json);
    }
};
