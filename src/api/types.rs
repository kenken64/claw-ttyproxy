//! Ollama-compatible API request and response types.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Chat
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub model: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub stream: Option<bool>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct ChatResponse {
    pub model: String,
    pub created_at: String,
    pub message: ChatMessage,
    pub done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_eval_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_eval_duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_duration: Option<u64>,
}

// ---------------------------------------------------------------------------
// Generate
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct GenerateRequest {
    pub model: Option<String>,
    pub prompt: String,
    pub stream: Option<bool>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct GenerateResponse {
    pub model: String,
    pub created_at: String,
    pub response: String,
    pub done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_eval_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_eval_duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_duration: Option<u64>,
}

// ---------------------------------------------------------------------------
// Show
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ShowRequest {
    pub model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ShowResponse {
    pub modelfile: String,
    pub parameters: String,
    pub template: String,
    pub details: ModelDetails,
}

// ---------------------------------------------------------------------------
// Tags (list models)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TagsResponse {
    pub models: Vec<ModelInfo>,
}

#[derive(Debug, Serialize)]
pub struct ModelInfo {
    pub name: String,
    pub model: String,
    pub modified_at: String,
    pub size: u64,
    pub digest: String,
    pub details: ModelDetails,
}

#[derive(Debug, Serialize, Clone)]
pub struct ModelDetails {
    pub parent_model: String,
    pub format: String,
    pub family: String,
    pub families: Vec<String>,
    pub parameter_size: String,
    pub quantization_level: String,
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub version: String,
}

// ---------------------------------------------------------------------------
// Embeddings
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct EmbeddingsRequest {
    pub model: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub input: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct EmbeddingsResponse {
    pub model: String,
    pub embeddings: Vec<Vec<f32>>,
    pub total_duration: u64,
    pub load_duration: u64,
    pub prompt_eval_count: u32,
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

impl ModelDetails {
    pub fn default_claude() -> Self {
        Self {
            parent_model: String::new(),
            format: "api".into(),
            family: "claude".into(),
            families: vec!["claude".into()],
            parameter_size: "unknown".into(),
            quantization_level: "none".into(),
        }
    }
}

impl ChatMessage {
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".into(),
            content: content.into(),
        }
    }
}

/// Convert a list of chat messages into a single prompt string for Claude CLI.
pub fn messages_to_prompt(messages: &[ChatMessage]) -> String {
    let mut parts = Vec::new();
    for msg in messages {
        match msg.role.as_str() {
            "system" => parts.push(format!("[System]\n{}", msg.content)),
            "user" => parts.push(msg.content.clone()),
            "assistant" => {
                parts.push(format!("[Previous assistant response]\n{}", msg.content))
            }
            _ => parts.push(msg.content.clone()),
        }
    }
    parts.join("\n\n")
}

pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn model_digest() -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"claude-code-ttyproxy");
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_messages_to_prompt_user_only() {
        let msgs = vec![ChatMessage {
            role: "user".into(),
            content: "Hello".into(),
        }];
        assert_eq!(messages_to_prompt(&msgs), "Hello");
    }

    #[test]
    fn test_messages_to_prompt_system_and_user() {
        let msgs = vec![
            ChatMessage {
                role: "system".into(),
                content: "Be helpful".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "Hi".into(),
            },
        ];
        let prompt = messages_to_prompt(&msgs);
        assert!(prompt.starts_with("[System]\nBe helpful"));
        assert!(prompt.contains("Hi"));
        assert!(prompt.contains("\n\n"));
    }

    #[test]
    fn test_messages_to_prompt_multi_turn() {
        let msgs = vec![
            ChatMessage {
                role: "user".into(),
                content: "Hello".into(),
            },
            ChatMessage {
                role: "assistant".into(),
                content: "Hi there!".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "How are you?".into(),
            },
        ];
        let prompt = messages_to_prompt(&msgs);
        assert!(prompt.contains("Hello"));
        assert!(prompt.contains("[Previous assistant response]\nHi there!"));
        assert!(prompt.contains("How are you?"));
    }

    #[test]
    fn test_messages_to_prompt_empty() {
        let msgs: Vec<ChatMessage> = vec![];
        assert_eq!(messages_to_prompt(&msgs), "");
    }

    #[test]
    fn test_messages_to_prompt_unknown_role() {
        let msgs = vec![ChatMessage {
            role: "tool".into(),
            content: "tool output".into(),
        }];
        assert_eq!(messages_to_prompt(&msgs), "tool output");
    }

    #[test]
    fn test_chat_message_assistant() {
        let msg = ChatMessage::assistant("test response");
        assert_eq!(msg.role, "assistant");
        assert_eq!(msg.content, "test response");
    }

    #[test]
    fn test_chat_message_assistant_from_string() {
        let msg = ChatMessage::assistant(String::from("owned string"));
        assert_eq!(msg.content, "owned string");
    }

    #[test]
    fn test_model_details_default() {
        let d = ModelDetails::default_claude();
        assert_eq!(d.family, "claude");
        assert_eq!(d.format, "api");
        assert_eq!(d.families, vec!["claude"]);
        assert!(d.parent_model.is_empty());
    }

    #[test]
    fn test_model_digest_deterministic() {
        let d1 = model_digest();
        let d2 = model_digest();
        assert_eq!(d1, d2);
        assert!(d1.starts_with("sha256:"));
        assert!(d1.len() > 10);
    }

    #[test]
    fn test_now_iso_format() {
        let ts = now_iso();
        // Should be a valid RFC3339 timestamp
        assert!(ts.contains('T'));
        assert!(ts.contains('+') || ts.ends_with('Z'));
    }
}
