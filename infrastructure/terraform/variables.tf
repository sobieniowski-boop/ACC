# ═══════════════════════════════════════════════════════════════════
# Variables
# ═══════════════════════════════════════════════════════════════════

variable "environment" {
  description = "Environment name (dev, staging, production)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be dev, staging, or production."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "polandcentral"
}

# ── Container Registry ────────────────────────────────────────────
variable "acr_sku" {
  description = "ACR SKU (Basic, Standard, Premium)"
  type        = string
  default     = "Basic"
}

# ── Container Instances ───────────────────────────────────────────
variable "api_cpu" {
  description = "CPU cores for API container"
  type        = number
  default     = 1
}

variable "api_memory" {
  description = "Memory (GB) for API container"
  type        = number
  default     = 1.5
}

variable "api_replicas" {
  description = "Number of API container instances"
  type        = number
  default     = 1
}

variable "worker_cpu" {
  description = "CPU cores for worker container"
  type        = number
  default     = 1
}

variable "worker_memory" {
  description = "Memory (GB) for worker container"
  type        = number
  default     = 1.5
}

variable "web_cpu" {
  description = "CPU cores for web (nginx) container"
  type        = number
  default     = 0.5
}

variable "web_memory" {
  description = "Memory (GB) for web container"
  type        = number
  default     = 0.5
}

# ── Networking ────────────────────────────────────────────────────
variable "vnet_address_space" {
  description = "VNET CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_aci_prefix" {
  description = "Subnet CIDR for ACI"
  type        = string
  default     = "10.0.1.0/24"
}

variable "subnet_private_prefix" {
  description = "Subnet CIDR for private endpoints"
  type        = string
  default     = "10.0.2.0/24"
}

# ── Key Vault ─────────────────────────────────────────────────────
variable "keyvault_sku" {
  description = "Key Vault SKU (standard, premium)"
  type        = string
  default     = "standard"
}

# ── Redis ─────────────────────────────────────────────────────────
variable "redis_capacity" {
  description = "Redis cache capacity (0-6 for Basic/Standard)"
  type        = number
  default     = 0
}

variable "redis_family" {
  description = "Redis family (C = Basic/Standard, P = Premium)"
  type        = string
  default     = "C"
}

variable "redis_sku" {
  description = "Redis SKU (Basic, Standard, Premium)"
  type        = string
  default     = "Basic"
}

# ── Monitoring ────────────────────────────────────────────────────
variable "log_retention_days" {
  description = "Log Analytics retention in days (30-730)"
  type        = number
  default     = 30
}

variable "alert_email" {
  description = "Email address for critical alert notifications"
  type        = string
  default     = "msobieniowski@users.noreply.github.com"
}

# ── Tags ──────────────────────────────────────────────────────────
variable "extra_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}
