# Copilot Instructions for Amazon Command Center (ACC)

## CRITICAL: Read Context First
Before doing ANY work, read `COPILOT_CONTEXT.md` in the workspace root.
It contains the full project architecture, credentials, current state, and known issues.

## Project Overview
ACC is a full-stack Amazon seller analytics platform (FastAPI + React + SQL Server).
The codebase is in `apps/api/` (Python backend) and `apps/web/` (React frontend).

## Communication
- Communicate with the user in **Polish** (native language)
- The user's name is Miłosz (msobieniowski)

## Technical Rules
1. Backend uses **raw pyodbc SQL** (not ORM queries) — especially `profit_engine.py`
2. All SQL reads use `WITH (NOLOCK)` hints
3. All SQL writes use `SET LOCK_TIMEOUT 30000`
4. Never run UPDATE/DELETE without WHERE clause
5. Config is in `.env` (read by pydantic-settings in `core/config.py`)
6. Frontend API client is in `apps/web/src/lib/api.ts`
7. User has **NO local admin rights** — don't try MSI installs or UAC elevation

## Current State (Mar 2026)
- **Azure SQL** fully operational (acc-sql-kadax.database.windows.net, user accadmin, read+write)
- **Dual-connection**: `connect_acc()` → Azure SQL (pymssql), `connect_netfox()` → ERP (pyodbc, read-only)
- **Order backfill**: 101K+ orders, 116K+ order lines via Reports API TSV
- **Amazon Ads API**: 10 profiles, 5,083 campaigns (SP/SB/SD), daily reports pending
- **Scheduler**: APScheduler in-process (orders 15min, purchase prices 02:00, finances 03:00, inventory 04:00, profit 05:00, ads 07:00)
- **Security**: No hardcoded credentials, `.gitignore` includes `ads_tokens.json`, `*.tokens.json`, `tmp_*.py`

## Key Files
- `apps/api/app/services/profit_engine.py` — CM1 profit calculation SQL engine
- `apps/api/app/services/ads_sync.py` — Amazon Ads sync pipeline
- `apps/api/app/connectors/amazon_ads_api/` — Ads API connector (client, profiles, campaigns, reporting)
- `apps/api/app/core/db_connection.py` — connect_acc() / connect_netfox() factory
- `apps/api/app/api/v1/profit_v2.py` — profit v2 API endpoints
- `apps/api/app/core/config.py` — settings/config
- `apps/web/src/lib/api.ts` — frontend API client
- `apps/web/src/pages/ProductProfitTable.tsx` — main profit dashboard
