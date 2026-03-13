# ═══════════════════════════════════════════════════════════════
# ACC — Production Environment
# ═══════════════════════════════════════════════════════════════

environment = "production"
location    = "polandcentral"

# Container Registry — Standard for production (geo-replication ready)
acr_sku = "Standard"

# ACI — production sizing per architecture spec §6.1
api_cpu       = 2
api_memory    = 2.0
api_replicas  = 2
worker_cpu    = 2
worker_memory = 2.0
web_cpu       = 0.5
web_memory    = 0.5

# Networking
vnet_address_space    = "10.30.0.0/16"
subnet_aci_prefix     = "10.30.1.0/24"
subnet_private_prefix = "10.30.2.0/24"

# Key Vault
keyvault_sku = "standard"

# Redis — Standard for production
redis_capacity = 1
redis_family   = "C"
redis_sku      = "Standard"
