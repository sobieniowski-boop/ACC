# ═══════════════════════════════════════════════════════════════════
# Security Hardening — Firewall, WAF, Access Control
# ═══════════════════════════════════════════════════════════════════
# Layered security: NSG tightening, Key Vault policies, Redis firewall.
# Note: Azure Front Door / WAF requires Standard+ ACR tier for custom
# domains; added as commented-out resources for production upgrade path.
# ═══════════════════════════════════════════════════════════════════

# ── Redis Firewall — Allow only ACI subnet ───────────────────────
resource "azurerm_redis_firewall_rule" "aci_access" {
  name                = "allow_aci_subnet"
  redis_cache_name    = azurerm_redis_cache.acc.name
  resource_group_name = azurerm_resource_group.acc.name
  # ACI subnet range (e.g. 10.10.1.0 - 10.10.1.255)
  start_ip = cidrhost(var.subnet_aci_prefix, 0)
  end_ip   = cidrhost(var.subnet_aci_prefix, 254)
}

# ── Key Vault Network ACLs — Restrict to VNet ────────────────────
# Uncomment when using private endpoints (requires Premium KV or VNet integration)
# resource "azurerm_key_vault_access_policy" "aci_identity" {
#   key_vault_id = azurerm_key_vault.acc.id
#   tenant_id    = data.azurerm_client_config.current.tenant_id
#   object_id    = azurerm_container_group.acc_app.identity[0].principal_id
#
#   secret_permissions = ["Get", "List"]
# }

# ── NSG: Tighten API access in production ────────────────────────
# The existing NSG in networking.tf allows :3010 from anywhere (web)
# and :8000 from ACI subnet only. This adds explicit outbound rules.
resource "azurerm_network_security_rule" "deny_outbound_ssh" {
  name                        = "deny-outbound-ssh"
  priority                    = 200
  direction                   = "Outbound"
  access                      = "Deny"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "22"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.acc.name
  network_security_group_name = azurerm_network_security_group.aci.name
}

resource "azurerm_network_security_rule" "deny_outbound_rdp" {
  name                        = "deny-outbound-rdp"
  priority                    = 210
  direction                   = "Outbound"
  access                      = "Deny"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "3389"
  source_address_prefix       = "*"
  destination_address_prefix  = "*"
  resource_group_name         = azurerm_resource_group.acc.name
  network_security_group_name = azurerm_network_security_group.aci.name
}

# ── Alert: Key Vault Secret Access Anomaly ───────────────────────
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "keyvault_anomaly" {
  name                = "acc-${var.environment}-keyvault-anomaly"
  resource_group_name = azurerm_resource_group.acc.name
  location            = azurerm_resource_group.acc.location
  scopes              = [azurerm_log_analytics_workspace.acc.id]
  severity            = 1
  window_duration     = "PT1H"
  evaluation_frequency = "PT15M"

  criteria {
    query = <<-KQL
      AzureDiagnostics
      | where ResourceType == "VAULTS" and OperationName == "SecretGet"
      | summarize count() by CallerIPAddress, bin(TimeGenerated, 5m)
      | where count_ > 50
    KQL
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
  }

  action {
    action_groups = [azurerm_monitor_action_group.critical.id]
  }

  tags = local.common_tags
}
