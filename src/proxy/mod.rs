pub mod bedrock;
pub mod claude;
pub mod stream;

use crate::api::types::{messages_to_prompt, ChatMessage};
use bedrock::BedrockRunner;
use claude::ClaudeRunner;
use tokio::sync::mpsc;

/// Unified backend: routes requests to either the Claude CLI or AWS Bedrock.
pub enum BackendRunner {
    Claude(ClaudeRunner),
    Bedrock(BedrockRunner),
}

impl BackendRunner {
    /// Single-turn prompt (used by /api/generate).
    pub async fn run_streaming(
        &self,
        prompt: &str,
        request_id: &str,
    ) -> Result<mpsc::Receiver<String>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Claude(r) => r.run_streaming(prompt, request_id).await,
            Self::Bedrock(r) => r.run_streaming(prompt, request_id).await,
        }
    }

    /// Single-turn prompt (used by /api/generate, non-streaming).
    pub async fn run_blocking(
        &self,
        prompt: &str,
        request_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Claude(r) => r.run_blocking(prompt, request_id).await,
            Self::Bedrock(r) => r.run_blocking(prompt, request_id).await,
        }
    }

    /// Multi-turn chat (used by /api/chat, streaming).
    /// Bedrock receives the messages array natively; Claude CLI flattens to a prompt.
    pub async fn run_streaming_chat(
        &self,
        messages: &[ChatMessage],
        request_id: &str,
    ) -> Result<mpsc::Receiver<String>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Claude(r) => {
                let prompt = messages_to_prompt(messages);
                r.run_streaming(&prompt, request_id).await
            }
            Self::Bedrock(r) => r.run_streaming_chat(messages, request_id).await,
        }
    }

    /// Multi-turn chat (used by /api/chat, non-streaming).
    pub async fn run_blocking_chat(
        &self,
        messages: &[ChatMessage],
        request_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Claude(r) => {
                let prompt = messages_to_prompt(messages);
                r.run_blocking(&prompt, request_id).await
            }
            Self::Bedrock(r) => r.run_blocking_chat(messages, request_id).await,
        }
    }
}
