# ═══════════════════════════════════════════════════════════════
# ACC — Development Environment
# ═══════════════════════════════════════════════════════════════

environment = "dev"
location    = "polandcentral"

# Container Registry — minimal for dev
acr_sku = "Basic"

# ACI — small footprint for dev
api_cpu       = 0.5
api_memory    = 1.0
api_replicas  = 1
worker_cpu    = 0.5
worker_memory = 1.0
web_cpu       = 0.25
web_memory    = 0.5

# Networking
vnet_address_space    = "10.10.0.0/16"
subnet_aci_prefix     = "10.10.1.0/24"
subnet_private_prefix = "10.10.2.0/24"

# Key Vault
keyvault_sku = "standard"

# Redis — Basic tier for dev
redis_capacity = 0
redis_family   = "C"
redis_sku      = "Basic"
