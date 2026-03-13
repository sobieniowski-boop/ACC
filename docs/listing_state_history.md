# Listing State History Tracking (P4)

## Overview

P4 adds persistent tracking of listing status transitions. Every time
`upsert_listing_state()` detects that `listing_status` has changed, a
history row is recorded in `acc_listing_state_history`.

The existing `acc_listing_state` table and its behavior are **unchanged**.

## Table: `acc_listing_state_history`

```sql
CREATE TABLE dbo.acc_listing_state_history (
    id                BIGINT IDENTITY(1,1) PRIMARY KEY,
    seller_sku        NVARCHAR(100)  NOT NULL,
    marketplace_id    VARCHAR(20)    NOT NULL,
    asin              VARCHAR(20)    NULL,
    previous_status   VARCHAR(30)    NULL,
    new_status        VARCHAR(30)    NOT NULL,
    issue_code        NVARCHAR(200)  NULL,
    issue_severity    VARCHAR(20)    NULL,
    changed_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    change_source     VARCHAR(50)    NOT NULL DEFAULT 'unknown',
    INDEX ix_lsh_sku_mkt_changed (seller_sku, marketplace_id, changed_at)
)
```

### Key design decisions

- **Append-only** — no updates or deletes, pure audit trail.
- **Same transaction** — history INSERT happens in the same cursor/commit
  as the state UPDATE, so the two are always consistent.
- **Fail-safe** — if the history table doesn't exist yet, the
  `ensure_listing_state_schema()` DDL auto-creates it.

## Change detection logic

```
upsert_listing_state():
    SELECT id, listing_status FROM acc_listing_state WHERE sku = ? AND mkt = ?
    ...
    UPDATE acc_listing_state ...
    IF listing_status IS NOT NULL AND old_status != listing_status:
        INSERT INTO acc_listing_state_history (...)
    COMMIT
```

Only records transitions when the caller provides a `listing_status` value
AND it differs from the current persisted status. This avoids noise from
upserts that only touch other fields (price, title, etc.).

## `change_source` values

| Value | Origin |
|---|---|
| `report` | Daily listing report sweep (`sync_listings_to_products`) |
| `notification` | Real-time SP-API notification via Event Backbone |
| `manual_refresh` | On-demand Listings Items API call per SKU |
| `unknown` | Fallback default |

The source is passed via the existing `sync_source` parameter of
`upsert_listing_state()`.

## `issue_code` extraction

When an `issues_snapshot` JSON is provided, the first issue's `code`
(or `issueType`) is extracted and stored in `issue_code`. This allows
querying transitions caused by specific Amazon issue types without
parsing the full JSON.

## Migration

Revision `eb004` (depends on `eb003`):

```
migrations/versions/20260310_listing_state_history.py
```

Apply: `alembic upgrade eb004`

Also auto-created by `ensure_listing_state_schema()` on app startup.

## Example queries

### Recent transitions for a SKU

```sql
SELECT previous_status, new_status, changed_at, change_source
FROM acc_listing_state_history WITH (NOLOCK)
WHERE seller_sku = 'MY-SKU-001'
  AND marketplace_id = 'A1PA6795UKMFR9'
ORDER BY changed_at DESC
```

### Suppression wave detection

```sql
SELECT CAST(changed_at AS DATE) AS day, COUNT(*) AS transitions
FROM acc_listing_state_history WITH (NOLOCK)
WHERE new_status = 'INACTIVE'
  AND changed_at > DATEADD(DAY, -30, GETUTCDATE())
GROUP BY CAST(changed_at AS DATE)
ORDER BY day DESC
```

### Transitions by source

```sql
SELECT change_source, COUNT(*) AS cnt
FROM acc_listing_state_history WITH (NOLOCK)
WHERE changed_at > DATEADD(DAY, -7, GETUTCDATE())
GROUP BY change_source
```
