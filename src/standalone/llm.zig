//! Public LLM facade for standalone mindbrain code.
//!
//! The implementation now lives in `llm/` so chat, streaming, tools,
//! multimodal parts, embeddings, and audio can share one provider-neutral API.

const lib = @import("llm/lib.zig");

pub const types = lib.types;
pub const http_client = lib.http_client;
pub const openai_compat = lib.openai_compat;
pub const gemini = lib.gemini;
pub const manager = lib.manager;

pub const ProviderKind = lib.ProviderKind;
pub const Capability = lib.Capability;
pub const ProviderConfig = lib.ProviderConfig;
pub const ManagerConfig = lib.ManagerConfig;
pub const ImageUrl = lib.ImageUrl;
pub const InlineData = lib.InlineData;
pub const ContentPart = lib.ContentPart;
pub const Message = lib.Message;
pub const Tool = lib.Tool;
pub const ToolChoice = lib.ToolChoice;
pub const ChatRequest = lib.ChatRequest;
pub const ChatOptions = lib.ChatOptions;
pub const ChatResponse = lib.ChatResponse;
pub const ToolCall = lib.ToolCall;
pub const ResponseFunctionCallOutput = lib.ResponseFunctionCallOutput;
pub const ResponseInputItem = lib.ResponseInputItem;
pub const ResponseInput = lib.ResponseInput;
pub const ResponseRequest = lib.ResponseRequest;
pub const ResponseOptions = lib.ResponseOptions;
pub const ResponseMessageOutput = lib.ResponseMessageOutput;
pub const ResponseFunctionCall = lib.ResponseFunctionCall;
pub const ResponseFunctionCallOutputOwned = lib.ResponseFunctionCallOutputOwned;
pub const ResponseReasoningOutput = lib.ResponseReasoningOutput;
pub const ResponseUnknownOutput = lib.ResponseUnknownOutput;
pub const ResponseOutputItem = lib.ResponseOutputItem;
pub const ResponseResult = lib.ResponseResult;
pub const StreamEvent = lib.StreamEvent;
pub const StreamEventKind = lib.StreamEventKind;
pub const EmbeddingRequest = lib.EmbeddingRequest;
pub const EmbeddingResponse = lib.EmbeddingResponse;
pub const EmbeddingVector = lib.EmbeddingVector;
pub const AudioTranscriptionRequest = lib.AudioTranscriptionRequest;
pub const AudioTranscriptionResponse = lib.AudioTranscriptionResponse;

pub const Manager = lib.Manager;
pub const supports = lib.supports;
pub const sanitizeToolName = lib.sanitizeToolName;
pub const restoreToolName = lib.restoreToolName;
pub const generateCacheKey = lib.generateCacheKey;
