# Handler Timeout & Circuit Breaker

**Date:** 2026-03-10
**Prompt:** P1

## Problem

A single event handler that hangs or repeatedly fails can block the entire
event processing loop, preventing other handlers (and other events) from
making progress.

## Solution

Two complementary mechanisms were added to `event_backbone.py`:

### 1. Handler timeout (30 s)

Every handler call is wrapped in a `concurrent.futures.ThreadPoolExecutor`
with a 30-second deadline.  If the handler does not return in time:

- The future is cancelled.
- A `TimeoutError` is raised.
- The attempt is logged with `handler_timeout = 1` in
  `acc_event_processing_log`.
- The event is **not** dropped — it stays in `received` for retry.

Constants (top of `event_backbone.py`):

```python
HANDLER_TIMEOUT_SECONDS = 30
```

### 2. Per-handler circuit breaker

State is stored in a new table `acc_event_handler_health`:

| Column | Type | Description |
|--------|------|-------------|
| handler_name | VARCHAR(100) | Unique handler identifier |
| failure_count | INT | Consecutive failures since last success |
| last_failure_at | DATETIME2 | Timestamp of most recent failure |
| circuit_open_until | DATETIME2 | When set and in the future, handler is skipped |

**Rules:**

- After **5 consecutive failures** → circuit opens for **15 minutes**.
- While the circuit is open, the handler is **skipped**, not invoked.
- A `skipped` row (with `circuit_open = 1`) is written to
  `acc_event_processing_log` for audit.
- The event stays in `status = 'received'` — it is **not dropped** and will
  be retried once the circuit closes.
- A single success **resets** `failure_count` to 0 and clears
  `circuit_open_until`.

Constants:

```python
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_COOLDOWN_MINUTES = 15
```

## New table

```sql
CREATE TABLE dbo.acc_event_handler_health (
    id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
    handler_name        VARCHAR(100)  NOT NULL UNIQUE,
    failure_count       INT           NOT NULL DEFAULT 0,
    last_failure_at     DATETIME2     NULL,
    circuit_open_until  DATETIME2     NULL,
    created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
```

## New columns on `acc_event_processing_log`

| Column | Type | Description |
|--------|------|-------------|
| handler_timeout | BIT (default 0) | 1 if this attempt failed due to timeout |
| circuit_open | BIT (default 0) | 1 if handler was skipped (circuit open) |

## Metrics in processing log

Each row in `acc_event_processing_log` now records:

- `duration_ms` — actual handler wall-clock time (existing)
- `handler_timeout` — whether the handler timed out
- `circuit_open` — whether the handler was skipped due to open circuit

## API surface

- `get_handler_health()` — returns list of handler health rows with
  computed `circuit_open` boolean.
- `get_backbone_health()` — now includes `open_circuits`,
  `handler_health`, `handler_timeout_seconds`,
  `circuit_breaker_threshold`, and `circuit_breaker_cooldown_min`.

## `process_pending_events` return shape

```python
{
    "processed": int,     # events fully handled
    "failed": int,        # events where handler(s) raised (non-circuit)
    "skipped": int,       # events with no registered handlers
    "circuit_skipped": int,  # events deferred due to open circuit
    "total": int,
}
```

## Backward compatibility

- If `acc_event_handler_health` does not exist at startup,
  `ensure_event_backbone_schema()` creates it automatically.
- The new columns on `acc_event_processing_log` are added via
  `IF NOT EXISTS` guards — safe for existing installations.
- All existing handler registrations continue to work unchanged.
- No changes to `ingest()`, `ingest_batch()`, `poll_sqs()`, `replay_events()`,
  query helpers, or destination/subscription persistence.

## Migration

Alembic migration: `migrations/versions/20260310_handler_circuit_breaker.py`
(revision `eb002`, depends on `fm001`).

## Files changed

- `apps/api/app/services/event_backbone.py` — timeout, circuit breaker, metrics
- `apps/api/migrations/versions/20260310_handler_circuit_breaker.py` — DDL migration
