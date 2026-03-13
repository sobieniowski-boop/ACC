# SQS Adaptive Polling Strategy (P3)

## Overview

P3 replaces the single-shot SQS poll with an adaptive loop that drains
the queue more aggressively during burst periods while keeping idle
overhead low.

## Previous Behavior

```
poll_sqs(max_messages=10)   # single call, max 10 messages
```

Scheduled every 2 minutes → max throughput **10 msg / 2 min = 5 msg/min**.

## New Behavior

```
for loop in range(MAX_POLL_LOOPS):     # default 5
    messages = sqs.receive_message(MaxNumberOfMessages=10)
    ingest each message
    if len(messages) < 10:
        break   # queue drained or nearly empty
```

Same 2-minute schedule → max throughput **50 msg / 2 min = 25 msg/min** (5× improvement).

### Early-exit rules

| Condition | Action |
|---|---|
| `messages == 0` | Break immediately (empty poll) |
| `messages < max_messages` | Break (queue nearly drained) |
| `loop == MAX_POLL_LOOPS - 1` | Stop (safety cap) |
| SQS API error | Return immediately with partial results |

## Constants

| Constant | Value | Location |
|---|---|---|
| `MAX_POLL_LOOPS` | 5 | `event_backbone.py` |
| SQS `MaxNumberOfMessages` | 10 | SQS hard limit |
| `WaitTimeSeconds` | 5 | Long-polling timeout per loop |

Worst-case wall time per cycle: `5 loops × 5s wait = 25s` (only if queue
has ≥ 50 messages).

## Delete-after-persist guarantee

The delete-only-on-success pattern is preserved:

```python
result = ingest(body, source="sqs", ...)

if result["status"] == "created":
    total_created += 1
elif result["status"] == "duplicate":
    total_duplicates += 1
else:
    total_errors += 1
    continue  # ← skip delete — SQS will redeliver

sqs.delete_message(...)  # only reached on success/duplicate
```

Error messages remain in SQS and will be redelivered after the
visibility timeout expires.

## Metrics

Three in-process counters are maintained in `_sqs_metrics`:

| Metric | Description |
|---|---|
| `sqs_messages_received` | Total messages received since app start |
| `sqs_poll_loops` | Total individual SQS API calls made |
| `sqs_empty_polls` | Polls that returned zero messages |

### Accessing metrics

- **API**: `GET /api/v1/notifications/sqs-metrics`
- **Health**: Included in `GET /api/v1/notifications/health` → `sqs_metrics` field
- **Code**: `event_backbone.get_sqs_metrics()`

### Deriving useful indicators

```
avg_messages_per_loop = sqs_messages_received / sqs_poll_loops
empty_poll_ratio      = sqs_empty_polls / sqs_poll_loops
```

A high `empty_poll_ratio` (> 0.8) suggests the polling interval could be
lengthened.  A consistently low ratio with frequent 5-loop cycles
suggests `MAX_POLL_LOOPS` should be increased.

## Response format

```json
{
  "status": "ok",
  "correlation_id": "a1b2c3...",
  "received": 37,
  "created": 35,
  "duplicates": 2,
  "errors": 0,
  "loops": 4,
  "empty_polls": 0,
  "max_loops": 5
}
```

## Tuning

To change the loop cap without code changes, override `MAX_POLL_LOOPS`
at the module level or pass `max_loops=N` to `poll_sqs()`.

The API endpoint at `POST /notifications/poll-sqs` currently uses the
default.  To expose it as a parameter, add a query param to the router.
