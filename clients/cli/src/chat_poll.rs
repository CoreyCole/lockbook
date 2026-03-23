use crate::sync_dir::{load_fs_base, FsBaseEntry};
use cli_rs::cli_error::{CliError, CliResult};
use lb_rs::model::chat::{Buffer, Message};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

struct ChatState {
    content_hash: [u8; 32],
    last_processed_ts: i64,
}

#[derive(serde::Serialize)]
struct WebhookPayload {
    thread: String,
    messages: Vec<WebhookMessage>,
    sync_dir: String,
    agent_name: String,
}

#[derive(serde::Serialize)]
struct WebhookMessage {
    from: String,
    content: String,
    ts: i64,
}

/// Extract .chat entries from .sync-dir-state, keyed by local_path.
fn chat_entries_from_fs_base(
    fs_base: &HashMap<Uuid, FsBaseEntry>,
) -> HashMap<&str, [u8; 32]> {
    fs_base
        .values()
        .filter(|e| e.local_path.ends_with(".chat"))
        .map(|e| (e.local_path.as_str(), e.content_hash))
        .collect()
}

fn find_new_messages(
    chat_path: &Path,
    agent_name: &str,
    fs_base_hash: [u8; 32],
    state: &mut ChatState,
) -> Vec<Message> {
    if fs_base_hash == state.content_hash {
        return vec![];
    }

    let content = fs::read(chat_path).unwrap_or_default();
    let buffer = Buffer::new(&content);
    let new_msgs: Vec<Message> = buffer
        .messages
        .iter()
        .filter(|m| m.ts > state.last_processed_ts && m.from != agent_name)
        .cloned()
        .collect();

    if let Some(latest) = buffer.messages.last() {
        state.last_processed_ts = latest.ts;
    }
    state.content_hash = fs_base_hash;

    new_msgs
}

fn initialize_state(local_dir: &Path, state: &mut HashMap<String, ChatState>) {
    let fs_base = load_fs_base(local_dir);
    for (_, entry) in &fs_base {
        if !entry.local_path.ends_with(".chat") {
            continue;
        }

        let chat_path = local_dir.join(&entry.local_path);
        let content = fs::read(&chat_path).unwrap_or_default();
        let buffer = Buffer::new(&content);
        let last_processed_ts = buffer.messages.last().map_or(0, |m| m.ts);

        state.insert(
            entry.local_path.clone(),
            ChatState { content_hash: entry.content_hash, last_processed_ts },
        );
    }
}

fn post_webhook(url: &str, payload: &WebhookPayload) -> Result<(), String> {
    let body = serde_json::to_string(payload).map_err(|e| e.to_string())?;
    ureq::post(url)
        .set("Content-Type", "application/json")
        .send_string(&body)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn poll_once(
    local_dir: &Path,
    agent_name: &str,
    webhook_url: &str,
    state: &mut HashMap<String, ChatState>,
) -> Result<(), String> {
    let fs_base = load_fs_base(local_dir);
    let chat_entries = chat_entries_from_fs_base(&fs_base);

    for (rel_path, fs_base_hash) in &chat_entries {
        let chat_state = state.entry(rel_path.to_string()).or_insert_with(|| {
            // New file discovered after initialization — dispatch all human messages
            ChatState { content_hash: [0; 32], last_processed_ts: 0 }
        });

        let chat_path = local_dir.join(rel_path);
        let new_msgs = find_new_messages(&chat_path, agent_name, *fs_base_hash, chat_state);

        if !new_msgs.is_empty() {
            let payload = WebhookPayload {
                thread: rel_path.to_string(),
                messages: new_msgs
                    .iter()
                    .map(|m| WebhookMessage {
                        from: m.from.clone(),
                        content: m.content.clone(),
                        ts: m.ts,
                    })
                    .collect(),
                sync_dir: local_dir.to_string_lossy().to_string(),
                agent_name: agent_name.to_string(),
            };

            post_webhook(webhook_url, &payload)?;
            println!("dispatched {} message(s) from {}", payload.messages.len(), rel_path);
        }
    }

    Ok(())
}

fn parse_duration(s: &str) -> CliResult<Duration> {
    let s = s.trim();
    if let Some(secs) = s.strip_suffix('s') {
        secs.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| CliError::from(format!("invalid duration: {s}")))
    } else if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| CliError::from(format!("invalid duration: {s}")))
    } else if let Some(m) = s.strip_suffix('m') {
        m.parse::<u64>()
            .map(|v| Duration::from_secs(v * 60))
            .map_err(|_| CliError::from(format!("invalid duration: {s}")))
    } else {
        s.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| {
                CliError::from(format!("invalid duration: {s} (expected e.g. 5s, 500ms, 1m)"))
            })
    }
}

#[tokio::main]
pub async fn run(
    local_dir: String,
    agent_name: String,
    webhook_url: String,
    poll_interval: Option<String>,
    once: bool,
) -> CliResult<()> {
    let local_dir = PathBuf::from(&local_dir);
    let mut state: HashMap<String, ChatState> = HashMap::new();

    initialize_state(&local_dir, &mut state);
    println!("chat-poll: tracking {} .chat file(s)", state.len());

    if once {
        poll_once(&local_dir, &agent_name, &webhook_url, &mut state)
            .map_err(CliError::from)?;
    } else {
        let interval = match poll_interval {
            Some(s) => parse_duration(&s)?,
            None => Duration::from_secs(5),
        };

        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = poll_once(&local_dir, &agent_name, &webhook_url, &mut state) {
                        eprintln!("chat-poll cycle failed: {e}");
                    }
                }
                _ = tokio::signal::ctrl_c() => break,
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_chat_file(dir: &Path, rel_path: &str, messages: &[(&str, &str, i64)]) {
        let path = dir.join(rel_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let content: String = messages
            .iter()
            .map(|(from, content, ts)| {
                serde_json::to_string(&serde_json::json!({
                    "from": from,
                    "content": content,
                    "ts": ts,
                }))
                .unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(path, content).unwrap();
    }

    fn write_fs_base(dir: &Path, entries: &[(&str, [u8; 32])]) {
        let map: HashMap<Uuid, FsBaseEntry> = entries
            .iter()
            .map(|(path, hash)| {
                (
                    Uuid::new_v4(),
                    FsBaseEntry {
                        local_path: path.to_string(),
                        content_hash: *hash,
                        lb_last_modified: 0,
                    },
                )
            })
            .collect();
        let data = serde_json::to_vec(&map).unwrap();
        fs::write(dir.join(".sync-dir-state"), data).unwrap();
    }

    #[test]
    fn find_new_messages_empty_when_hash_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        write_chat_file(dir.path(), "test.chat", &[("alice", "hello", 100)]);

        let hash = [1u8; 32];
        let mut state = ChatState { content_hash: hash, last_processed_ts: 100 };

        // Same hash as state — no change detected
        let msgs = find_new_messages(&dir.path().join("test.chat"), "bot", hash, &mut state);
        assert!(msgs.is_empty());
    }

    #[test]
    fn find_new_messages_detects_new_human_message() {
        let dir = tempfile::tempdir().unwrap();
        write_chat_file(dir.path(), "test.chat", &[("alice", "hello", 100)]);

        let mut state = ChatState { content_hash: [0; 32], last_processed_ts: 0 };

        // Different hash triggers re-read
        let msgs = find_new_messages(&dir.path().join("test.chat"), "bot", [1; 32], &mut state);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].from, "alice");
        assert_eq!(msgs[0].content, "hello");
    }

    #[test]
    fn find_new_messages_skips_agent_messages() {
        let dir = tempfile::tempdir().unwrap();
        write_chat_file(
            dir.path(),
            "test.chat",
            &[("alice", "hello", 100), ("bot", "hi there", 101)],
        );

        let mut state = ChatState { content_hash: [0; 32], last_processed_ts: 0 };

        let msgs = find_new_messages(&dir.path().join("test.chat"), "bot", [1; 32], &mut state);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].from, "alice");
    }

    #[test]
    fn find_new_messages_respects_last_processed_ts() {
        let dir = tempfile::tempdir().unwrap();
        write_chat_file(
            dir.path(),
            "test.chat",
            &[("alice", "old", 100), ("alice", "new", 200)],
        );

        let mut state = ChatState { content_hash: [0; 32], last_processed_ts: 150 };

        let msgs = find_new_messages(&dir.path().join("test.chat"), "bot", [1; 32], &mut state);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].content, "new");
    }

    #[test]
    fn initialize_state_from_fs_base() {
        let dir = tempfile::tempdir().unwrap();
        write_chat_file(dir.path(), "a.chat", &[("alice", "hello", 100)]);
        write_chat_file(
            dir.path(),
            "sub/b.chat",
            &[("bob", "hey", 200), ("bob", "there", 300)],
        );
        write_fs_base(dir.path(), &[("a.chat", [1; 32]), ("sub/b.chat", [2; 32])]);

        let mut state = HashMap::new();
        initialize_state(dir.path(), &mut state);

        assert_eq!(state.len(), 2);
        assert_eq!(state["a.chat"].last_processed_ts, 100);
        assert_eq!(state["a.chat"].content_hash, [1; 32]);
        assert_eq!(state["sub/b.chat"].last_processed_ts, 300);
        assert_eq!(state["sub/b.chat"].content_hash, [2; 32]);
    }

    #[test]
    fn initialize_state_ignores_non_chat_files() {
        let dir = tempfile::tempdir().unwrap();
        write_fs_base(dir.path(), &[("readme.md", [1; 32]), ("a.chat", [2; 32])]);
        write_chat_file(dir.path(), "a.chat", &[("alice", "hi", 100)]);

        let mut state = HashMap::new();
        initialize_state(dir.path(), &mut state);

        assert_eq!(state.len(), 1);
        assert!(state.contains_key("a.chat"));
    }
}
