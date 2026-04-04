//! Mock `claude` CLI binary for testing.
//!
//! Supports the same flags as the real `claude -p`:
//!   mock-claude -p --output-format text
//!   mock-claude -p --output-format stream-json
//!   mock-claude -p --output-format stream-json --dangerously-skip-permissions
//!
//! Reads the prompt from stdin, returns a deterministic response.
//! The response includes the prompt length so tests can verify round-trip.

use std::io::Read;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Parse --output-format value
    let output_format = args
        .windows(2)
        .find(|w| w[0] == "--output-format")
        .map(|w| w[1].as_str())
        .unwrap_or("text");

    // Read full prompt from stdin
    let mut prompt = String::new();
    std::io::stdin().read_to_string(&mut prompt).unwrap();

    let has_skip_perms = args.iter().any(|a| a == "--dangerously-skip-permissions");

    // Build a deterministic response
    let response_text = format!(
        "Mock response to prompt ({} bytes). dangerously_skip_permissions={}. Echo: {}",
        prompt.len(),
        has_skip_perms,
        prompt.chars().take(100).collect::<String>()
    );

    match output_format {
        "text" => {
            println!("{response_text}");
        }
        "stream-json" => {
            // Emit events matching the claude stream-json format.
            // Split the response into word-sized chunks to simulate streaming.
            let words: Vec<&str> = response_text.split_inclusive(' ').collect();

            // message_start
            println!(r#"{{"type":"message_start"}}"#);

            // content_block_start
            println!(r#"{{"type":"content_block_start","index":0,"content_block":{{"type":"text","text":""}}}}"#);

            // content_block_delta for each chunk
            for word in &words {
                let escaped = word.replace('\\', "\\\\").replace('"', "\\\"");
                println!(
                    r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"text_delta","text":"{escaped}"}}}}"#
                );
                // Small delay to simulate streaming (not strictly needed for tests)
            }

            // content_block_stop
            println!(r#"{{"type":"content_block_stop","index":0}}"#);

            // message_stop
            println!(r#"{{"type":"message_stop"}}"#);

            // result with full text
            let full_escaped = response_text.replace('\\', "\\\\").replace('"', "\\\"");
            println!(r#"{{"type":"result","result":"{full_escaped}"}}"#);
        }
        other => {
            eprintln!("mock-claude: unknown output format: {other}");
            std::process::exit(1);
        }
    }
}
