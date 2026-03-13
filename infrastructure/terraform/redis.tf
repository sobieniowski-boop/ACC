# ═══════════════════════════════════════════════════════════════════
# Azure Cache for Redis
# ═══════════════════════════════════════════════════════════════════
# Dev: Docker-based Redis (skip this resource)
# Staging/Production: Azure Cache for Redis
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_redis_cache" "acc" {
  name                = "acc-${var.environment}-redis"
  location            = azurerm_resource_group.acc.location
  resource_group_name = azurerm_resource_group.acc.name
  capacity            = var.redis_capacity
  family              = var.redis_family
  sku_name            = var.redis_sku
  non_ssl_port_enabled = false
  minimum_tls_version = "1.2"

  redis_configuration {
    maxmemory_policy = "allkeys-lru"
  }

  tags = local.common_tags
}
