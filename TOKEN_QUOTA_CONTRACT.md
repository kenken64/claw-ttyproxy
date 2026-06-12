# Token Usage & Quota Contract (ttyproxy ⇄ 2ndBrain)

This is the source-of-truth contract for the Redis messages exchanged between
**ttyproxy** (this service) and **2ndBrain**. 2ndBrain is the authority for
quota; ttyproxy only observes Bedrock usage, publishes it, and enforces the
quota state 2ndBrain pushes back.

Behaviour described here is implemented in `src/usage.rs`
(`record_bedrock_usage`, `usage_payload`, `apply_quota_payload`) and
`src/api/handlers.rs` (the quota preflight). Keep this doc in sync with that code.

## Flow

```
ttyproxy  --usage-->  openclaw:token_usage:v1   -->  2ndBrain (store usage, increment llm_token_used)
2ndBrain  --quota-->  2ndbrain:token-quota       -->  ttyproxy (update local quota state, enforce)
```

- **Usage channel** (ttyproxy publishes): `openclaw:token_usage:v1`
  - override: `TOKEN_USAGE_REDIS_CHANNEL` (default shown)
- **Quota channel** (ttyproxy subscribes): `2ndbrain:token-quota`
  - override: `TOKEN_QUOTA_REDIS_CHANNEL` (default shown)
- Redis connection: `TOKEN_QUOTA_REDIS_URL` (falls back to `TOKEN_USAGE_REDIS_URL`).
- Instance identity: `OPENCLAW_INSTANCE` — stamped into every usage payload and
  used to match inbound quota events. Must be the real, stable instance id.

---

## 1. Usage events — published BY ttyproxy

Every billable Bedrock completion (`/api/chat`, `/api/generate`, streaming and
non-streaming) is recorded to a durable SQLite ledger and published to
`openclaw:token_usage:v1`. Zero-token and failed requests are not published.

```json
{
  "type": "openclaw.token_usage.v1",
  "event_id": "3dd135b1-6241-4dff-8fe0-20eb86942f83",
  "request_id": "req-13",
  "provider": "aws_bedrock",
  "endpoint": "/api/chat",
  "model": "global.anthropic.claude-sonnet-4-6",
  "openclaw_instance": "openclaw-bcd56ecb",
  "profile_id": null,
  "input_tokens": 12,
  "output_tokens": 4,
  "cache_creation_input_tokens": 0,
  "cache_read_input_tokens": 0,
  "total_tokens": 16,
  "llm_token_used_delta": 16,
  "is_streaming": false,
  "created_at": "2026-06-12T10:45:58.939743659+00:00"
}
```

`llm_token_used_delta` is the amount 2ndBrain should add to `llm_token_used`.
When ttyproxy has a known quota snapshot, the payload also carries
`observed_llm_token_used`, `remaining_tokens`, and `llm_token_quota`.

Delivery is fire-and-forget Redis pub/sub (no backlog): 2ndBrain must hold a
live `SUBSCRIBE` on this exact channel/instance or events are dropped. ttyproxy
retries any event still unpublished in its ledger on a flush loop.

---

## 2. Quota events — consumed BY ttyproxy

2ndBrain publishes to `2ndbrain:token-quota`. ttyproxy upserts the value into
its local `token_quota_state` and enforces it on the **next** request (read
fresh from SQLite, no restart needed).

### Canonical event

```json
{
  "event": "token_quota.updated",
  "openclaw_instance": "openclaw-bcd56ecb",
  "llmTokenQuota": 1000000,
  "llmTokenUsed": 734512,
  "remainingTokens": 265488,
  "source": "2ndBrain.ceo",
  "reason": "quota_sync",
  "occurredAt": "2026-06-12T11:00:00.000Z",
  "version": 1
}
```

**Enforcement rule:** Bedrock is blocked when `remainingTokens <= 0`, allowed
when `> 0`. Keep `remainingTokens = llmTokenQuota − llmTokenUsed`.

### Disable an instance

```json
{
  "event": "token_quota.updated",
  "openclaw_instance": "openclaw-bcd56ecb",
  "llmTokenQuota": 0,
  "llmTokenUsed": 0,
  "remainingTokens": 0,
  "source": "2ndBrain.ceo",
  "reason": "admin_disable"
}
```

### Re-enable an instance

```json
{
  "event": "token_quota.updated",
  "openclaw_instance": "openclaw-bcd56ecb",
  "llmTokenQuota": 1000000,
  "llmTokenUsed": 0,
  "remainingTokens": 1000000,
  "source": "2ndBrain.ceo",
  "reason": "admin_enable"
}
```

### Field reference

| Field | Required | Accepted aliases | Notes |
|---|---|---|---|
| `event` | yes\* | — | must be `"token_quota.updated"`; other values ignored. \*May be omitted but send it. |
| `openclaw_instance` | **yes** | `openclawInstance`, `openclaw_instance_id`, `instance`, `instance_id`, `metadata.openclaw_instance` | must equal the target instance id. Required whenever ttyproxy has no `PROFILE_ID`. |
| `llmTokenQuota` | one of these 3 | `llm_token_quota`, `quota` | total allotment |
| `llmTokenUsed` | one of these 3 | `llm_token_used`, `used` | consumed |
| `remainingTokens` | one of these 3 | `remaining_tokens`, `availableTokens`, `available_tokens`, `remaining` | the gate value |
| `profile_id` | optional | `profileId`, `user_id`, `userId`, `userID`, nested in `metadata` | used for matching only if instance is absent |
| `source` | optional | — | free text |
| other (`actor`, `deltaTokens`, `email`, `metadata`, `version`, …) | optional | — | ignored — keep your standard envelope |

### Semantics

- At least one of quota / used / remaining must be present, else the event is
  rejected (`"quota update must include quota, used, or remaining tokens"`).
- **Precedence:** if `quota` is present, ttyproxy computes
  `remaining = quota − used`; the `remaining` field is only a fallback used when
  no quota is known. Always send the consistent triple for deterministic results.
- Upsert overwrites the stored row for the instance; effective on the next
  request; no restart.
- Always include `openclaw_instance`. If omitted, ttyproxy applies the event to
  its own configured instance by default (when `PROFILE_ID` is unset) — explicit
  targeting is safer.
- Enforcement is gated by `TOKEN_USAGE_ENFORCE_QUOTA` (default `true`). Set it
  explicitly in production.

### UX note

While `remainingTokens <= 0`, ttyproxy returns a normal `200` with the assistant
message *"This OpenClaw instance has used all assigned AI credits … top up in
2ndBrain"* and does not call Bedrock (no tokens spent). It is **not** an HTTP
error. A distinct "disabled by admin" status would require a code change.
