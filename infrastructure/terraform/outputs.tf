# ═══════════════════════════════════════════════════════════════════
# Outputs
# ═══════════════════════════════════════════════════════════════════

output "resource_group_name" {
  value = azurerm_resource_group.acc.name
}

output "acr_login_server" {
  value = azurerm_container_registry.acc.login_server
}

output "acr_admin_username" {
  value     = azurerm_container_registry.acc.admin_username
  sensitive = true
}

output "aci_fqdn" {
  value = azurerm_container_group.acc_app.fqdn
}

output "aci_ip_address" {
  value = azurerm_container_group.acc_app.ip_address
}

output "key_vault_uri" {
  value = azurerm_key_vault.acc.vault_uri
}

output "redis_hostname" {
  value = azurerm_redis_cache.acc.hostname
}

output "redis_ssl_port" {
  value = azurerm_redis_cache.acc.ssl_port
}

# ── Monitoring ────────────────────────────────────────────────────
output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.acc.id
}

output "appinsights_instrumentation_key" {
  value     = azurerm_application_insights.acc.instrumentation_key
  sensitive = true
}

output "appinsights_connection_string" {
  value     = azurerm_application_insights.acc.connection_string
  sensitive = true
}
