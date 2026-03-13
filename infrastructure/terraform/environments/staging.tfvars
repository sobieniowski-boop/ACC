# ═══════════════════════════════════════════════════════════════
# ACC — Staging Environment
# ═══════════════════════════════════════════════════════════════

environment = "staging"
location    = "polandcentral"

# Container Registry
acr_sku = "Basic"

# ACI — mirrors production sizing
api_cpu       = 1
api_memory    = 1.5
api_replicas  = 1
worker_cpu    = 1
worker_memory = 1.5
web_cpu       = 0.5
web_memory    = 0.5

# Networking
vnet_address_space    = "10.20.0.0/16"
subnet_aci_prefix     = "10.20.1.0/24"
subnet_private_prefix = "10.20.2.0/24"

# Key Vault
keyvault_sku = "standard"

# Redis — Standard for staging
redis_capacity = 0
redis_family   = "C"
redis_sku      = "Standard"
