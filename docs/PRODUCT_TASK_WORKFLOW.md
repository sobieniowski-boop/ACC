# Product Task Workflow (SLA + Auto Assignment)

This document describes the operational workflow implemented for Product Tasks:
- task queue (`open`, `investigating`, `resolved`)
- comments and owner assignment
- SLA alerts
- auto owner assignment by marketplace/brand/task type rules

## 1) Data Model

Tables:
- `dbo.acc_al_product_tasks`
- `dbo.acc_al_product_task_comments`
- `dbo.acc_al_task_owner_rules`
- `dbo.acc_al_alert_rules` (system SLA rules are auto-created)
- `dbo.acc_al_alerts` (SLA breaches are written here)

Important columns:
- `acc_al_product_tasks.status`: `open | investigating | resolved`
- `acc_al_product_tasks.owner`: task owner
- `acc_al_task_owner_rules.priority`: lower value = higher precedence

## 2) API Endpoints

Task lifecycle:
- `POST /api/v1/profit/v2/tasks` create task
- `GET /api/v1/profit/v2/tasks` list tasks (filters: `status`, `task_type`, `owner`, `sku_search`, pagination)
- `PATCH /api/v1/profit/v2/tasks/{task_id}` update `status` / `owner` / `title` / `note`

Comments:
- `GET /api/v1/profit/v2/tasks/{task_id}/comments`
- `POST /api/v1/profit/v2/tasks/{task_id}/comments`

Owner auto-assignment rules:
- `GET /api/v1/profit/v2/tasks/owner-rules`
- `POST /api/v1/profit/v2/tasks/owner-rules`
- `DELETE /api/v1/profit/v2/tasks/owner-rules/{rule_id}`

## 3) Auto Assignment Logic

When a task is created and `owner` is not provided:
1. System tries to detect SKU brand (`acc_product` / order-line mapping).
2. System matches active rules from `acc_al_task_owner_rules`.
3. Rule matching dimensions:
- `task_type` (exact or `NULL`)
- `marketplace_id` (exact or `NULL`)
- `brand` (exact or `NULL`)
4. Winning rule order:
- lowest `priority` first
- then more specific rules (brand/marketplace/task_type non-null) before generic rules

If no rule matches, `owner` remains empty.

## 4) SLA Alert Logic

SLA checks run as part of alert evaluation:
- `open` task older than `48h` -> `critical` alert
- `investigating` task older than `72h` -> `warning` alert

Alert deduplication:
- one unresolved SLA alert per task/rule in a rolling 24h window

System SLA rules are auto-created if missing:
- `Task SLA Open > 48h` (`rule_type=task_sla_open`)
- `Task SLA Investigating > 72h` (`rule_type=task_sla_investigating`)

## 5) Scheduler

New scheduler job:
- `evaluate-alerts-hourly` (every 60 minutes)
- evaluates normal alert rules + task SLA alerts

Job type:
- `evaluate_alerts` (added to allowed job types in MSSQL store)

## 6) Frontend

New screen:
- `/profit/tasks` -> Product Tasks

Capabilities:
- task list with filters
- inline status change
- owner assignment
- comment thread per task
- owner-rule management (create/delete)

## 7) Operational Notes

- Startup schema bootstrap (`ensure_v2_schema`) creates/extends required tables.
- Existing installations are upgraded safely:
- missing `owner` column is added to `acc_al_product_tasks`
- missing tables are created idempotently
