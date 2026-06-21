//! Bedrock token usage tracking.
//!
//! 2ndBrain.ceo remains the source of truth for quota. This module keeps a
//! durable local ledger of observed Bedrock usage, publishes usage deltas to
//! Redis, and listens for quota state updates from 2ndBrain.

use futures::StreamExt;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, info, warn};
use uuid::Uuid;

type UsageResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone)]
pub struct TokenUsageConfig {
    pub db_path: PathBuf,
    pub redis_url: Option<String>,
    pub usage_channel: String,
    pub quota_channel: String,
    pub openclaw_instance: String,
    pub profile_id: Option<String>,
    pub enforce_quota: bool,
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct TokenUsage {
    pub provider: String,
    pub endpoint: String,
    pub request_id: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_creation_input_tokens: i64,
    pub cache_read_input_tokens: i64,
    pub is_streaming: bool,
}

impl TokenUsage {
    pub fn total_tokens(&self) -> i64 {
        self.input_tokens
            + self.output_tokens
            + self.cache_creation_input_tokens
            + self.cache_read_input_tokens
    }
}

#[derive(Debug, Clone)]
pub struct QuotaSnapshot {
    pub openclaw_instance: String,
    pub profile_id: Option<String>,
    pub llm_token_quota: Option<i64>,
    pub llm_token_used: i64,
    pub remaining_tokens: Option<i64>,
    pub openclaw_tokens_paused: bool,
    pub openclaw_tokens_paused_at: Option<String>,
    pub openclaw_tokens_pause_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaBlockReason {
    Exhausted,
    Paused,
}

#[derive(Debug, Clone)]
pub struct QuotaExceeded {
    pub snapshot: QuotaSnapshot,
    pub reason: QuotaBlockReason,
}

pub struct TokenUsageTracker {
    config: TokenUsageConfig,
    db: Arc<Mutex<Connection>>,
    redis_client: Option<redis::Client>,
}

impl TokenUsageTracker {
    pub fn open(config: TokenUsageConfig) -> UsageResult<Arc<Self>> {
        if let Some(parent) = config
            .db_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&config.db_path)?;
        initialize_schema(&conn)?;

        let redis_client = match &config.redis_url {
            Some(url) => Some(redis::Client::open(url.as_str())?),
            None => None,
        };

        let tracker = Arc::new(Self {
            config,
            db: Arc::new(Mutex::new(conn)),
            redis_client,
        });

        info!(
            db_path = %tracker.config.db_path.display(),
            redis_enabled = tracker.redis_client.is_some(),
            usage_channel = %tracker.config.usage_channel,
            quota_channel = %tracker.config.quota_channel,
            openclaw_instance = %tracker.config.openclaw_instance,
            enforce_quota = tracker.config.enforce_quota,
            "token usage tracker initialized"
        );

        Ok(tracker)
    }

    pub fn start_background_tasks(self: &Arc<Self>) {
        if self.redis_client.is_none() {
            return;
        }

        let quota_tracker = Arc::clone(self);
        tokio::spawn(async move {
            quota_tracker.quota_listener_loop().await;
        });

        let flush_tracker = Arc::clone(self);
        tokio::spawn(async move {
            flush_tracker.pending_publish_loop().await;
        });
    }

    pub fn enforces_quota(&self) -> bool {
        self.config.enforce_quota
    }

    pub fn quota_exceeded(&self) -> UsageResult<Option<QuotaExceeded>> {
        if !self.config.enforce_quota {
            return Ok(None);
        }

        let Some(snapshot) = self.quota_snapshot()? else {
            return Ok(None);
        };

        if snapshot.openclaw_tokens_paused {
            return Ok(Some(QuotaExceeded {
                snapshot,
                reason: QuotaBlockReason::Paused,
            }));
        }

        if matches!(snapshot.remaining_tokens, Some(remaining) if remaining <= 0) {
            return Ok(Some(QuotaExceeded {
                snapshot,
                reason: QuotaBlockReason::Exhausted,
            }));
        }

        Ok(None)
    }

    pub fn quota_snapshot(&self) -> UsageResult<Option<QuotaSnapshot>> {
        self.quota_snapshot_for_instance(&self.config.openclaw_instance)
    }

    pub async fn record_bedrock_usage(&self, usage: TokenUsage) -> UsageResult<()> {
        let total_tokens = usage.total_tokens();
        if total_tokens <= 0 {
            debug!(
                request_id = %usage.request_id,
                endpoint = %usage.endpoint,
                "skipping zero-token usage record"
            );
            return Ok(());
        }

        let event_id = self.insert_usage_event(&usage)?;

        if self.redis_client.is_some() {
            if let Err(error) = self.publish_event_id(&event_id).await {
                self.mark_publish_failed(&event_id, &error.to_string())?;
                warn!(
                    request_id = %usage.request_id,
                    event_id = %event_id,
                    error = %error,
                    "failed to publish token usage event"
                );
            }
        }

        Ok(())
    }

    fn insert_usage_event(&self, usage: &TokenUsage) -> UsageResult<String> {
        let event_id = Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now().to_rfc3339();
        let total_tokens = usage.total_tokens();
        let profile_id = self.config.profile_id.as_deref();
        let mut conn = self.db.lock().expect("token usage database mutex poisoned");
        let tx = conn.transaction()?;

        tx.execute(
            "insert into token_usage_events (
                event_id,
                request_id,
                provider,
                endpoint,
                model,
                openclaw_instance,
                profile_id,
                input_tokens,
                output_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
                is_streaming,
                created_at
            ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                event_id,
                usage.request_id.as_str(),
                usage.provider.as_str(),
                usage.endpoint.as_str(),
                usage.model.as_str(),
                self.config.openclaw_instance.as_str(),
                profile_id,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
                total_tokens,
                if usage.is_streaming { 1 } else { 0 },
                created_at,
            ],
        )?;

        tx.execute(
            "insert into token_usage_totals (
                openclaw_instance,
                profile_id,
                total_input_tokens,
                total_output_tokens,
                total_cache_creation_input_tokens,
                total_cache_read_input_tokens,
                total_tokens,
                updated_at
            ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            on conflict(openclaw_instance) do update set
                profile_id = coalesce(excluded.profile_id, token_usage_totals.profile_id),
                total_input_tokens = token_usage_totals.total_input_tokens + excluded.total_input_tokens,
                total_output_tokens = token_usage_totals.total_output_tokens + excluded.total_output_tokens,
                total_cache_creation_input_tokens = token_usage_totals.total_cache_creation_input_tokens + excluded.total_cache_creation_input_tokens,
                total_cache_read_input_tokens = token_usage_totals.total_cache_read_input_tokens + excluded.total_cache_read_input_tokens,
                total_tokens = token_usage_totals.total_tokens + excluded.total_tokens,
                updated_at = excluded.updated_at",
            params![
                self.config.openclaw_instance.as_str(),
                profile_id,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
                total_tokens,
                created_at,
            ],
        )?;

        // NOTE: the proxy deliberately does NOT deduct quota locally here.
        // 2ndBrain.ceo is the single source of truth for quota: we record the
        // usage event + observed totals and publish the delta to Redis, then
        // 2ndBrain applies the deduction and pushes the authoritative
        // `token_quota_state` back via the quota channel (see apply_quota_payload).
        tx.commit()?;

        info!(
            request_id = %usage.request_id,
            event_id = %event_id,
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            cache_creation_input_tokens = usage.cache_creation_input_tokens,
            cache_read_input_tokens = usage.cache_read_input_tokens,
            total_tokens,
            openclaw_instance = %self.config.openclaw_instance,
            "bedrock token usage recorded"
        );

        Ok(event_id)
    }

    async fn pending_publish_loop(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_millis(
            self.config.flush_interval_ms.max(1_000),
        ));

        loop {
            ticker.tick().await;
            if let Err(error) = self.publish_pending_events().await {
                warn!(error = %error, "token usage pending publish pass failed");
            }
        }
    }

    async fn publish_pending_events(&self) -> UsageResult<()> {
        let event_ids = {
            let conn = self.db.lock().expect("token usage database mutex poisoned");
            let mut stmt = conn.prepare(
                "select event_id
                 from token_usage_events
                 where redis_published_at is null
                 order by id asc
                 limit 100",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        for event_id in event_ids {
            if let Err(error) = self.publish_event_id(&event_id).await {
                self.mark_publish_failed(&event_id, &error.to_string())?;
                warn!(event_id = %event_id, error = %error, "failed to publish pending usage event");
            }
        }

        Ok(())
    }

    async fn publish_event_id(&self, event_id: &str) -> UsageResult<()> {
        let Some(client) = &self.redis_client else {
            return Ok(());
        };

        let payload = self.usage_payload(event_id)?;
        let payload_json = serde_json::to_string(&payload)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("PUBLISH")
            .arg(&self.config.usage_channel)
            .arg(payload_json)
            .query_async(&mut conn)
            .await?;
        self.mark_published(event_id)?;
        Ok(())
    }

    fn mark_published(&self, event_id: &str) -> UsageResult<()> {
        let published_at = chrono::Utc::now().to_rfc3339();
        let conn = self.db.lock().expect("token usage database mutex poisoned");
        conn.execute(
            "update token_usage_events
             set redis_published_at = ?1,
                 publish_attempts = publish_attempts + 1,
                 last_publish_error = null
             where event_id = ?2",
            params![published_at, event_id],
        )?;
        Ok(())
    }

    fn mark_publish_failed(&self, event_id: &str, error: &str) -> UsageResult<()> {
        let conn = self.db.lock().expect("token usage database mutex poisoned");
        conn.execute(
            "update token_usage_events
             set publish_attempts = publish_attempts + 1,
                 last_publish_error = ?1
             where event_id = ?2",
            params![error, event_id],
        )?;
        Ok(())
    }

    fn usage_payload(&self, event_id: &str) -> UsageResult<Value> {
        let conn = self.db.lock().expect("token usage database mutex poisoned");
        let usage = conn.query_row(
            "select
                event_id,
                request_id,
                provider,
                endpoint,
                model,
                openclaw_instance,
                profile_id,
                input_tokens,
                output_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
                is_streaming,
                created_at
             from token_usage_events
             where event_id = ?1",
            params![event_id],
            |row| {
                Ok(UsagePayloadRow {
                    event_id: row.get(0)?,
                    request_id: row.get(1)?,
                    provider: row.get(2)?,
                    endpoint: row.get(3)?,
                    model: row.get(4)?,
                    openclaw_instance: row.get(5)?,
                    profile_id: row.get(6)?,
                    input_tokens: row.get(7)?,
                    output_tokens: row.get(8)?,
                    cache_creation_input_tokens: row.get(9)?,
                    cache_read_input_tokens: row.get(10)?,
                    total_tokens: row.get(11)?,
                    is_streaming: row.get::<_, i64>(12)? != 0,
                    created_at: row.get(13)?,
                })
            },
        )?;

        let quota = self.quota_snapshot_for_instance_locked(&conn, &usage.openclaw_instance)?;
        let mut payload = json!({
            "type": "openclaw.token_usage.v1",
            "event_id": usage.event_id,
            "request_id": usage.request_id,
            "provider": usage.provider,
            "endpoint": usage.endpoint,
            "model": usage.model,
            "openclaw_instance": usage.openclaw_instance,
            "profile_id": usage.profile_id,
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_creation_input_tokens": usage.cache_creation_input_tokens,
            "cache_read_input_tokens": usage.cache_read_input_tokens,
            "total_tokens": usage.total_tokens,
            "llm_token_used_delta": usage.total_tokens,
            "is_streaming": usage.is_streaming,
            "created_at": usage.created_at,
        });

        if let (Some(object), Some(quota)) = (payload.as_object_mut(), quota) {
            object.insert(
                "observed_llm_token_used".into(),
                json!(quota.llm_token_used),
            );
            object.insert("remaining_tokens".into(), json!(quota.remaining_tokens));
            object.insert("llm_token_quota".into(), json!(quota.llm_token_quota));
            object.insert(
                "openclaw_tokens_paused".into(),
                json!(quota.openclaw_tokens_paused),
            );
        }

        Ok(payload)
    }

    async fn quota_listener_loop(self: Arc<Self>) {
        loop {
            if let Err(error) = self.listen_for_quota_once().await {
                warn!(
                    error = %error,
                    quota_channel = %self.config.quota_channel,
                    "redis quota listener disconnected"
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    async fn listen_for_quota_once(&self) -> UsageResult<()> {
        let Some(client) = &self.redis_client else {
            return Ok(());
        };

        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.subscribe(&self.config.quota_channel).await?;

        info!(
            quota_channel = %self.config.quota_channel,
            openclaw_instance = %self.config.openclaw_instance,
            "listening for token quota updates"
        );

        let mut stream = pubsub.on_message();
        while let Some(message) = stream.next().await {
            let payload: String = message.get_payload()?;
            match self.apply_quota_payload(&payload) {
                Ok(true) => debug!("applied token quota update"),
                Ok(false) => debug!("ignored token quota update for another instance"),
                Err(error) => warn!(error = %error, "invalid token quota update"),
            }
        }

        Ok(())
    }

    fn apply_quota_payload(&self, payload: &str) -> UsageResult<bool> {
        let value: Value = serde_json::from_str(payload)?;
        let event_name = string_field(&value, &["event"]);
        if matches!(event_name.as_deref(), Some(event) if event != "token_quota.updated") {
            return Ok(false);
        }

        let incoming_instance = string_field(
            &value,
            &[
                "openclaw_instance",
                "openclawInstance",
                "openclaw_instance_id",
                "instance",
                "instance_id",
            ],
        )
        .or_else(|| {
            nested_string_field(
                &value,
                "metadata",
                &["openclaw_instance", "openclawInstance"],
            )
        });

        if matches!(incoming_instance.as_deref(), Some(instance) if instance != self.config.openclaw_instance)
        {
            return Ok(false);
        }

        let incoming_profile_id = string_field(
            &value,
            &["profile_id", "profileId", "user_id", "userId", "userID"],
        )
        .or_else(|| {
            nested_string_field(
                &value,
                "metadata",
                &["profile_id", "profileId", "user_id", "userId", "userID"],
            )
        });

        if incoming_instance.is_none() {
            if let Some(configured_profile_id) = self.config.profile_id.as_deref() {
                if incoming_profile_id.as_deref() != Some(configured_profile_id) {
                    return Ok(false);
                }
            }
        }

        let incoming_instance =
            incoming_instance.unwrap_or_else(|| self.config.openclaw_instance.clone());
        let profile_id = incoming_profile_id.or_else(|| self.config.profile_id.clone());
        let quota = int_field(&value, &["llm_token_quota", "llmTokenQuota", "quota"]);
        let used = int_field(&value, &["llm_token_used", "llmTokenUsed", "used"]);
        let remaining = int_field(
            &value,
            &[
                "remaining_tokens",
                "remainingTokens",
                "available_tokens",
                "availableTokens",
                "remaining",
            ],
        );
        let paused = bool_field(
            &value,
            &[
                "openclaw_tokens_paused",
                "openclawTokensPaused",
                "tokens_paused",
                "tokensPaused",
                "paused",
            ],
        )
        .or_else(|| {
            nested_bool_field(
                &value,
                "metadata",
                &[
                    "openclaw_tokens_paused",
                    "openclawTokensPaused",
                    "tokens_paused",
                    "tokensPaused",
                    "paused",
                ],
            )
        });
        let paused_at = string_field(
            &value,
            &[
                "openclaw_tokens_paused_at",
                "openclawTokensPausedAt",
                "tokens_paused_at",
                "tokensPausedAt",
                "pausedAt",
            ],
        )
        .or_else(|| {
            nested_string_field(
                &value,
                "metadata",
                &[
                    "openclaw_tokens_paused_at",
                    "openclawTokensPausedAt",
                    "tokens_paused_at",
                    "tokensPausedAt",
                    "pausedAt",
                ],
            )
        });
        let pause_reason = string_field(
            &value,
            &[
                "openclaw_tokens_pause_reason",
                "openclawTokensPauseReason",
                "tokens_pause_reason",
                "tokensPauseReason",
                "pauseReason",
            ],
        )
        .or_else(|| {
            nested_string_field(
                &value,
                "metadata",
                &[
                    "openclaw_tokens_pause_reason",
                    "openclawTokensPauseReason",
                    "tokens_pause_reason",
                    "tokensPauseReason",
                    "pauseReason",
                ],
            )
        });

        if quota.is_none() && used.is_none() && remaining.is_none() && paused.is_none() {
            return Err("quota update must include quota, used, remaining, or paused state".into());
        }

        let existing = self.quota_snapshot_for_instance(&incoming_instance)?;
        let next_quota = quota.or_else(|| existing.as_ref().and_then(|s| s.llm_token_quota));
        let next_used = used
            .or_else(|| match (next_quota, remaining) {
                (Some(quota), Some(remaining)) => Some(quota - remaining),
                _ => existing.as_ref().map(|s| s.llm_token_used),
            })
            .unwrap_or(0);
        let next_remaining = next_quota.map(|quota| quota - next_used).or(remaining);
        let received_at = chrono::Utc::now().to_rfc3339();
        let next_paused = paused
            .or_else(|| existing.as_ref().map(|s| s.openclaw_tokens_paused))
            .unwrap_or(false);
        let next_paused_at = if next_paused {
            paused_at
                .or_else(|| {
                    existing
                        .as_ref()
                        .and_then(|s| s.openclaw_tokens_paused_at.clone())
                })
                .or_else(|| Some(received_at.clone()))
        } else {
            None
        };
        let next_pause_reason = if next_paused {
            pause_reason
                .or_else(|| {
                    existing
                        .as_ref()
                        .and_then(|s| s.openclaw_tokens_pause_reason.clone())
                })
                .or_else(|| Some("openclaw_tokens_paused".into()))
        } else {
            None
        };
        let source = string_field(&value, &["source"]).unwrap_or_else(|| "redis".into());

        let conn = self.db.lock().expect("token usage database mutex poisoned");
        conn.execute(
            "insert into token_quota_state (
                openclaw_instance,
                profile_id,
                llm_token_quota,
                llm_token_used,
                remaining_tokens,
                openclaw_tokens_paused,
                openclaw_tokens_paused_at,
                openclaw_tokens_pause_reason,
                source,
                received_at,
                raw_payload
            ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            on conflict(openclaw_instance) do update set
                profile_id = coalesce(excluded.profile_id, token_quota_state.profile_id),
                llm_token_quota = excluded.llm_token_quota,
                llm_token_used = excluded.llm_token_used,
                remaining_tokens = excluded.remaining_tokens,
                openclaw_tokens_paused = excluded.openclaw_tokens_paused,
                openclaw_tokens_paused_at = excluded.openclaw_tokens_paused_at,
                openclaw_tokens_pause_reason = excluded.openclaw_tokens_pause_reason,
                source = excluded.source,
                received_at = excluded.received_at,
                raw_payload = excluded.raw_payload",
            params![
                incoming_instance.as_str(),
                profile_id,
                next_quota,
                next_used,
                next_remaining,
                if next_paused { 1_i64 } else { 0_i64 },
                next_paused_at,
                next_pause_reason,
                source,
                received_at,
                payload,
            ],
        )?;

        info!(
            openclaw_instance = %self.config.openclaw_instance,
            llm_token_quota = ?next_quota,
            llm_token_used = next_used,
            remaining_tokens = ?next_remaining,
            openclaw_tokens_paused = next_paused,
            "token quota state updated"
        );

        Ok(true)
    }

    fn quota_snapshot_for_instance(&self, instance: &str) -> UsageResult<Option<QuotaSnapshot>> {
        let conn = self.db.lock().expect("token usage database mutex poisoned");
        self.quota_snapshot_for_instance_locked(&conn, instance)
    }

    fn quota_snapshot_for_instance_locked(
        &self,
        conn: &Connection,
        instance: &str,
    ) -> UsageResult<Option<QuotaSnapshot>> {
        conn.query_row(
            "select openclaw_instance,
                    profile_id,
                    llm_token_quota,
                    llm_token_used,
                    remaining_tokens,
                    openclaw_tokens_paused,
                    openclaw_tokens_paused_at,
                    openclaw_tokens_pause_reason
             from token_quota_state
             where openclaw_instance = ?1",
            params![instance],
            |row| {
                Ok(QuotaSnapshot {
                    openclaw_instance: row.get(0)?,
                    profile_id: row.get(1)?,
                    llm_token_quota: row.get(2)?,
                    llm_token_used: row.get(3)?,
                    remaining_tokens: row.get(4)?,
                    openclaw_tokens_paused: row.get::<_, i64>(5)? != 0,
                    openclaw_tokens_paused_at: row.get(6)?,
                    openclaw_tokens_pause_reason: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }
}

#[derive(Debug)]
struct UsagePayloadRow {
    event_id: String,
    request_id: String,
    provider: String,
    endpoint: String,
    model: String,
    openclaw_instance: String,
    profile_id: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_creation_input_tokens: i64,
    cache_read_input_tokens: i64,
    total_tokens: i64,
    is_streaming: bool,
    created_at: String,
}

fn initialize_schema(conn: &Connection) -> UsageResult<()> {
    conn.execute_batch(
        "
        pragma journal_mode = wal;
        pragma foreign_keys = on;

        create table if not exists token_usage_events (
            id integer primary key autoincrement,
            event_id text not null unique,
            request_id text not null,
            provider text not null,
            endpoint text not null,
            model text not null,
            openclaw_instance text not null,
            profile_id text,
            input_tokens integer not null default 0,
            output_tokens integer not null default 0,
            cache_creation_input_tokens integer not null default 0,
            cache_read_input_tokens integer not null default 0,
            total_tokens integer not null,
            is_streaming integer not null default 0,
            created_at text not null,
            redis_published_at text,
            publish_attempts integer not null default 0,
            last_publish_error text
        );

        create index if not exists token_usage_events_request_idx
            on token_usage_events(request_id);

        create index if not exists token_usage_events_pending_publish_idx
            on token_usage_events(redis_published_at, id);

        create table if not exists token_usage_totals (
            openclaw_instance text primary key,
            profile_id text,
            total_input_tokens integer not null default 0,
            total_output_tokens integer not null default 0,
            total_cache_creation_input_tokens integer not null default 0,
            total_cache_read_input_tokens integer not null default 0,
            total_tokens integer not null default 0,
            updated_at text not null
        );

        create table if not exists token_quota_state (
            openclaw_instance text primary key,
            profile_id text,
            llm_token_quota integer,
            llm_token_used integer not null default 0,
            remaining_tokens integer,
            source text not null,
            received_at text not null,
            raw_payload text
        );
        ",
    )?;
    ensure_column(
        conn,
        "token_quota_state",
        "openclaw_tokens_paused",
        "integer not null default 0",
    )?;
    ensure_column(
        conn,
        "token_quota_state",
        "openclaw_tokens_paused_at",
        "text",
    )?;
    ensure_column(
        conn,
        "token_quota_state",
        "openclaw_tokens_pause_reason",
        "text",
    )?;
    Ok(())
}

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> UsageResult<()> {
    let mut stmt = conn.prepare(&format!("pragma table_info({table})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;

    for row in rows {
        if row? == column {
            return Ok(());
        }
    }

    conn.execute(
        &format!("alter table {table} add column {column} {definition}"),
        [],
    )?;
    Ok(())
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::String(s) => {
                let trimmed = s.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            }
            _ => None,
        })
    })
}

fn nested_string_field(value: &Value, parent: &str, names: &[&str]) -> Option<String> {
    value
        .get(parent)
        .and_then(|nested| string_field(nested, names))
}

fn bool_field(value: &Value, names: &[&str]) -> Option<bool> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::Bool(value) => Some(*value),
            Value::Number(number) => number.as_i64().map(|value| value != 0),
            Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            },
            _ => None,
        })
    })
}

fn nested_bool_field(value: &Value, parent: &str, names: &[&str]) -> Option<bool> {
    value
        .get(parent)
        .and_then(|nested| bool_field(nested, names))
}

fn int_field(value: &Value, names: &[&str]) -> Option<i64> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
            Value::String(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(db_path: PathBuf) -> TokenUsageConfig {
        TokenUsageConfig {
            db_path,
            redis_url: None,
            usage_channel: "openclaw:token_usage:v1".into(),
            quota_channel: "2ndbrain:token-quota".into(),
            openclaw_instance: "test-openclaw".into(),
            profile_id: Some("profile-1".into()),
            enforce_quota: true,
            flush_interval_ms: 1_000,
        }
    }

    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ttyproxy-{name}-{}.sqlite3", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn records_usage_without_advancing_local_quota_state() {
        // The proxy must NOT deduct quota locally; 2ndBrain owns the deduction.
        // Recording usage writes the ledger/totals and publishes the delta, but
        // leaves token_quota_state untouched until 2ndBrain pushes an update.
        let tracker = TokenUsageTracker::open(test_config(temp_db_path("usage"))).unwrap();
        tracker
            .apply_quota_payload(
                r#"{"type":"openclaw.token_quota.v1","openclaw_instance":"test-openclaw","llm_token_quota":100,"llm_token_used":10}"#,
            )
            .unwrap();

        tracker
            .record_bedrock_usage(TokenUsage {
                provider: "aws_bedrock".into(),
                endpoint: "/api/chat".into(),
                request_id: "req-1".into(),
                model: "model".into(),
                input_tokens: 20,
                output_tokens: 5,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                is_streaming: false,
            })
            .await
            .unwrap();

        // Quota state is unchanged by local usage: still the last value 2ndBrain pushed.
        let snapshot = tracker.quota_snapshot().unwrap().unwrap();
        assert_eq!(snapshot.llm_token_quota, Some(100));
        assert_eq!(snapshot.llm_token_used, 10);
        assert_eq!(snapshot.remaining_tokens, Some(90));
        assert!(tracker.quota_exceeded().unwrap().is_none());

        // But the usage was still recorded locally (ledger + observed totals).
        let observed: i64 = {
            let conn = tracker.db.lock().unwrap();
            conn.query_row(
                "select total_tokens from token_usage_totals where openclaw_instance = ?1",
                params!["test-openclaw"],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(observed, 25);
    }

    #[test]
    fn quota_exceeded_when_remaining_is_zero() {
        let tracker = TokenUsageTracker::open(test_config(temp_db_path("quota"))).unwrap();
        tracker
            .apply_quota_payload(
                r#"{"openclaw_instance":"test-openclaw","llm_token_quota":"10","llm_token_used":"10"}"#,
            )
            .unwrap();

        let exceeded = tracker.quota_exceeded().unwrap().unwrap();
        assert_eq!(exceeded.reason, QuotaBlockReason::Exhausted);
        assert_eq!(exceeded.snapshot.remaining_tokens, Some(0));
    }

    #[test]
    fn quota_exceeded_when_tokens_are_paused() {
        let tracker = TokenUsageTracker::open(test_config(temp_db_path("paused"))).unwrap();
        tracker
            .apply_quota_payload(
                r#"{
                    "openclaw_instance":"test-openclaw",
                    "llm_token_quota":100,
                    "llm_token_used":10,
                    "openclawTokensPaused":true,
                    "openclawTokensPausedAt":"2026-06-21T10:00:00Z",
                    "openclawTokensPauseReason":"user_pause"
                }"#,
            )
            .unwrap();

        let exceeded = tracker.quota_exceeded().unwrap().unwrap();
        assert_eq!(exceeded.reason, QuotaBlockReason::Paused);
        assert_eq!(exceeded.snapshot.remaining_tokens, Some(90));
        assert!(exceeded.snapshot.openclaw_tokens_paused);
        assert_eq!(
            exceeded.snapshot.openclaw_tokens_paused_at.as_deref(),
            Some("2026-06-21T10:00:00Z")
        );
        assert_eq!(
            exceeded.snapshot.openclaw_tokens_pause_reason.as_deref(),
            Some("user_pause")
        );
    }

    #[test]
    fn unpause_preserves_existing_quota_state() {
        let tracker =
            TokenUsageTracker::open(test_config(temp_db_path("resume-preserves-quota"))).unwrap();
        tracker
            .apply_quota_payload(
                r#"{
                    "openclaw_instance":"test-openclaw",
                    "llm_token_quota":100,
                    "llm_token_used":30,
                    "openclawTokensPaused":true
                }"#,
            )
            .unwrap();
        tracker
            .apply_quota_payload(
                r#"{
                    "openclaw_instance":"test-openclaw",
                    "openclawTokensPaused":false
                }"#,
            )
            .unwrap();

        let snapshot = tracker.quota_snapshot().unwrap().unwrap();
        assert_eq!(snapshot.llm_token_quota, Some(100));
        assert_eq!(snapshot.llm_token_used, 30);
        assert_eq!(snapshot.remaining_tokens, Some(70));
        assert!(!snapshot.openclaw_tokens_paused);
        assert!(snapshot.openclaw_tokens_paused_at.is_none());
        assert!(snapshot.openclaw_tokens_pause_reason.is_none());
        assert!(tracker.quota_exceeded().unwrap().is_none());
    }

    #[test]
    fn ignores_quota_updates_for_other_instances() {
        let tracker = TokenUsageTracker::open(test_config(temp_db_path("ignore"))).unwrap();
        let applied = tracker
            .apply_quota_payload(
                r#"{"openclaw_instance":"other-openclaw","llm_token_quota":10,"llm_token_used":0}"#,
            )
            .unwrap();

        assert!(!applied);
        assert!(tracker.quota_snapshot().unwrap().is_none());
    }

    #[test]
    fn applies_2ndbrain_token_quota_event_by_profile_id() {
        let tracker = TokenUsageTracker::open(test_config(temp_db_path("2ndbrain"))).unwrap();
        let applied = tracker
            .apply_quota_payload(
                r#"{
                    "actor": {"email": "admin@example.com", "userId": "admin-user-id"},
                    "availableTokens": 75,
                    "deltaTokens": 25,
                    "email": "user@example.com",
                    "event": "token_quota.updated",
                    "llmTokenQuota": 100,
                    "llmTokenUsed": 25,
                    "metadata": {},
                    "occurredAt": "2026-06-12T00:00:00.000Z",
                    "reason": "admin_quota_update",
                    "source": "2ndBrain.ceo",
                    "userId": "profile-1",
                    "version": 1
                }"#,
            )
            .unwrap();

        assert!(applied);
        let snapshot = tracker.quota_snapshot().unwrap().unwrap();
        assert_eq!(snapshot.profile_id.as_deref(), Some("profile-1"));
        assert_eq!(snapshot.llm_token_quota, Some(100));
        assert_eq!(snapshot.llm_token_used, 25);
        assert_eq!(snapshot.remaining_tokens, Some(75));
    }

    #[test]
    fn ignores_2ndbrain_token_quota_event_for_other_profile() {
        let tracker =
            TokenUsageTracker::open(test_config(temp_db_path("2ndbrain-ignore"))).unwrap();
        let applied = tracker
            .apply_quota_payload(
                r#"{
                    "event": "token_quota.updated",
                    "llmTokenQuota": 100,
                    "llmTokenUsed": 25,
                    "source": "2ndBrain.ceo",
                    "userId": "someone-else",
                    "version": 1
                }"#,
            )
            .unwrap();

        assert!(!applied);
        assert!(tracker.quota_snapshot().unwrap().is_none());
    }
}
