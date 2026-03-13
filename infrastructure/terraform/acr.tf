# ═══════════════════════════════════════════════════════════════════
# Azure Container Registry
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_container_registry" "acc" {
  name                = "accregistry${var.environment}"
  resource_group_name = azurerm_resource_group.acc.name
  location            = azurerm_resource_group.acc.location
  sku                 = var.acr_sku
  admin_enabled       = true

  tags = local.common_tags
}
