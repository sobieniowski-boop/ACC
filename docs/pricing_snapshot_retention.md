# Pricing Snapshot Retention (P5)

## Overview

P5 prevents unbounded growth of `acc_pricing_snapshot` by archiving
rows older than 30 days into a dedicated archive table. The archive
runs daily as a scheduled job and operates in batches to avoid lock
escalation.

## Tables

### `acc_pricing_snapshot` (existing)

Primary table for active pricing observations. Unchanged.

### `acc_pricing_snapshot_archive` (new)

Same schema as the main table, minus the computed `price_vs_buybox_pct`
column (not needed for historical analysis). Adds `archived_at` column.

```sql
CREATE TABLE dbo.acc_pricing_snapshot_archive (
    id                   BIGINT          NOT NULL PRIMARY KEY,
    seller_sku           NVARCHAR(100)   NOT NULL,
    asin                 VARCHAR(20)     NULL,
    marketplace_id       VARCHAR(20)     NOT NULL,
    our_price            DECIMAL(12,2)   NULL,
    our_currency         VARCHAR(5)      NOT NULL DEFAULT 'EUR',
    fulfillment_channel  VARCHAR(10)     NULL,
    buybox_price         DECIMAL(12,2)   NULL,
    buybox_landed_price  DECIMAL(12,2)   NULL,
    has_buybox           BIT             NOT NULL DEFAULT 0,
    is_featured_merchant BIT             NOT NULL DEFAULT 0,
    buybox_seller_id     VARCHAR(20)     NULL,
    lowest_price_new     DECIMAL(12,2)   NULL,
    num_offers_new       INT             NULL,
    num_offers_used      INT             NULL,
    bsr_rank             INT             NULL,
    bsr_category         NVARCHAR(200)   NULL,
    source               VARCHAR(30)     NOT NULL DEFAULT 'competitive_pricing_api',
    observed_at          DATETIME2       NOT NULL,
    created_at           DATETIME2       NOT NULL,
    archived_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
)
```

Indexes:
- `IX_pricing_snap_archive_observed` — `(observed_at DESC)`
- `IX_pricing_snap_archive_sku_mkt` — `(seller_sku, marketplace_id, observed_at DESC)`

## Archive function

```python
archive_old_snapshots(days=30) -> {"archived": int, "batches": int, "retention_days": int}
```

Located in `app/services/pricing_state.py`.

### Algorithm

```
WHILE True:
    INSERT TOP(10000) INTO archive
    SELECT FROM acc_pricing_snapshot
    WHERE observed_at < DATEADD(DAY, -30, GETUTCDATE())
      AND id NOT IN (SELECT id FROM archive)

    DELETE TOP(10000) FROM acc_pricing_snapshot
    WHERE observed_at < cutoff AND id IN archive

    IF inserted < batch_size: BREAK
```

### Safety features

| Feature | Detail |
|---|---|
| Batch size | 10,000 rows per iteration |
| Lock timeout | `SET LOCK_TIMEOUT 30000` (30s) |
| Idempotent | `NOT IN archive` prevents double-insert on retry |
| Transactional | INSERT and DELETE each in separate commits |
| Fail-safe | Rollback on exception, partial progress preserved |

## Scheduler

Registered in `app/scheduler.py`:

```python
scheduler.add_job(
    _archive_pricing_snapshots,
    trigger=CronTrigger(hour=2, minute=0),
    id="pricing-snapshot-archive-daily",
    name="Pricing Snapshot Archive (02:00)",
    ...
)
```

Runs daily at **02:00** (Europe/Warsaw timezone), after purchase price
sync (02:00) has completed — the archive only touches rows > 30 days
old so there's no conflict with fresh data.

Creates a job record in `acc_al_jobs` for UI visibility.

## Migration

Revision `eb005` (depends on `eb004`):

```
migrations/versions/20260310_pricing_snapshot_archive.py
```

Apply: `alembic upgrade eb005`

Also auto-created by `ensure_pricing_state_schema()` on app startup.

## Constants

| Constant | Value | Location |
|---|---|---|
| `DEFAULT_ARCHIVE_DAYS` | 30 | `pricing_state.py` |
| `ARCHIVE_BATCH_SIZE` | 10,000 | `pricing_state.py` |

## Manual trigger

```python
from app.services.pricing_state import archive_old_snapshots
result = archive_old_snapshots(days=30)
# {"archived": 45200, "batches": 5, "retention_days": 30}
```

Or with a custom retention period:

```python
result = archive_old_snapshots(days=60)  # keep 60 days in main table
```

## Query archived data

```sql
-- Recent archives
SELECT TOP 100 *
FROM acc_pricing_snapshot_archive WITH (NOLOCK)
ORDER BY archived_at DESC

-- Historical price for a SKU
SELECT observed_at, our_price, buybox_price
FROM acc_pricing_snapshot_archive WITH (NOLOCK)
WHERE seller_sku = 'MY-SKU-001'
  AND marketplace_id = 'A1PA6795UKMFR9'
ORDER BY observed_at DESC
```
