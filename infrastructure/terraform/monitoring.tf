# ═══════════════════════════════════════════════════════════════════
# Azure Monitor — Log Analytics + Application Insights
# ═══════════════════════════════════════════════════════════════════
# Centralized observability: logs, metrics, traces, alerts.
# Costs: Free tier ~5GB/month ingestion; Basic plan recommended for dev.
# ═══════════════════════════════════════════════════════════════════

# ── Log Analytics Workspace ──────────────────────────────────────
resource "azurerm_log_analytics_workspace" "acc" {
  name                = "acc-${var.environment}-logs"
  location            = azurerm_resource_group.acc.location
  resource_group_name = azurerm_resource_group.acc.name
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days

  tags = local.common_tags
}

# ── Application Insights (connected to Log Analytics) ────────────
resource "azurerm_application_insights" "acc" {
  name                = "acc-${var.environment}-appinsights"
  location            = azurerm_resource_group.acc.location
  resource_group_name = azurerm_resource_group.acc.name
  workspace_id        = azurerm_log_analytics_workspace.acc.id
  application_type    = "web"

  tags = local.common_tags
}

# ── Diagnostic Settings — ACI Container Group ────────────────────
resource "azurerm_monitor_diagnostic_setting" "aci_logs" {
  name                       = "acc-aci-diagnostics"
  target_resource_id         = azurerm_container_group.acc_app.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.acc.id

  enabled_log {
    category = "ContainerInstanceLog"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# ── Diagnostic Settings — Redis ──────────────────────────────────
resource "azurerm_monitor_diagnostic_setting" "redis_logs" {
  name                       = "acc-redis-diagnostics"
  target_resource_id         = azurerm_redis_cache.acc.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.acc.id

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# ── Diagnostic Settings — Key Vault ──────────────────────────────
resource "azurerm_monitor_diagnostic_setting" "keyvault_logs" {
  name                       = "acc-keyvault-diagnostics"
  target_resource_id         = azurerm_key_vault.acc.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.acc.id

  enabled_log {
    category = "AuditEvent"
  }

  metric {
    category = "AllMetrics"
    enabled  = true
  }
}

# ── Action Group — Alert Notifications ───────────────────────────
resource "azurerm_monitor_action_group" "critical" {
  name                = "acc-${var.environment}-critical-alerts"
  resource_group_name = azurerm_resource_group.acc.name
  short_name          = "AccCrit"

  email_receiver {
    name          = "ops-email"
    email_address = var.alert_email
  }
}

# ── Alert: ACI CPU > 80% ────────────────────────────────────────
resource "azurerm_monitor_metric_alert" "aci_high_cpu" {
  name                = "acc-${var.environment}-aci-high-cpu"
  resource_group_name = azurerm_resource_group.acc.name
  scopes              = [azurerm_container_group.acc_app.id]
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.ContainerInstance/containerGroups"
    metric_name      = "CpuUsage"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 80
  }

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }

  tags = local.common_tags
}

# ── Alert: ACI Memory > 85% ─────────────────────────────────────
resource "azurerm_monitor_metric_alert" "aci_high_memory" {
  name                = "acc-${var.environment}-aci-high-memory"
  resource_group_name = azurerm_resource_group.acc.name
  scopes              = [azurerm_container_group.acc_app.id]
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.ContainerInstance/containerGroups"
    metric_name      = "MemoryUsage"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 85
  }

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }

  tags = local.common_tags
}

# ── Alert: Redis Memory > 80% ───────────────────────────────────
resource "azurerm_monitor_metric_alert" "redis_high_memory" {
  name                = "acc-${var.environment}-redis-high-memory"
  resource_group_name = azurerm_resource_group.acc.name
  scopes              = [azurerm_redis_cache.acc.id]
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Cache/redis"
    metric_name      = "usedmemorypercentage"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 80
  }

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }

  tags = local.common_tags
}

# ── Alert: Redis Server Load > 70% ──────────────────────────────
resource "azurerm_monitor_metric_alert" "redis_high_load" {
  name                = "acc-${var.environment}-redis-high-load"
  resource_group_name = azurerm_resource_group.acc.name
  scopes              = [azurerm_redis_cache.acc.id]
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Cache/redis"
    metric_name      = "serverLoad"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 70
  }

  action {
    action_group_id = azurerm_monitor_action_group.critical.id
  }

  tags = local.common_tags
}
