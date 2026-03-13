# ═══════════════════════════════════════════════════════════════════
# Azure Key Vault — Secrets Management
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_key_vault" "acc" {
  name                       = "acc-${var.environment}-kv"
  location                   = azurerm_resource_group.acc.location
  resource_group_name        = azurerm_resource_group.acc.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = var.keyvault_sku
  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Purge"
    ]
  }

  tags = local.common_tags
}

# ── Secret References (must be pre-populated in Key Vault) ────────
# These are read by the ACI container group for secure env injection.
# Populate secrets via: az keyvault secret set --vault-name <name> --name <key> --value <value>

data "azurerm_key_vault_secret" "mssql_server" {
  name         = "mssql-server"
  key_vault_id = azurerm_key_vault.acc.id
}

data "azurerm_key_vault_secret" "mssql_user" {
  name         = "mssql-user"
  key_vault_id = azurerm_key_vault.acc.id
}

data "azurerm_key_vault_secret" "mssql_password" {
  name         = "mssql-password"
  key_vault_id = azurerm_key_vault.acc.id
}

data "azurerm_key_vault_secret" "mssql_database" {
  name         = "mssql-database"
  key_vault_id = azurerm_key_vault.acc.id
}

data "azurerm_key_vault_secret" "secret_key" {
  name         = "app-secret-key"
  key_vault_id = azurerm_key_vault.acc.id
}
