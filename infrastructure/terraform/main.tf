# ═══════════════════════════════════════════════════════════════════
# ACC Infrastructure — Terraform Main
# ═══════════════════════════════════════════════════════════════════
# Usage:
#   cd infrastructure/terraform
#   terraform init
#   terraform plan -var-file=environments/dev.tfvars
#   terraform apply -var-file=environments/dev.tfvars
# ═══════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.64"
    }
  }

  # Uncomment for remote state (recommended for team/production)
  # backend "azurerm" {
  #   resource_group_name  = "acc-tfstate-rg"
  #   storage_account_name = "acctfstate"
  #   container_name       = "tfstate"
  #   key                  = "acc.terraform.tfstate"
  # }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

# ── Data Sources ──────────────────────────────────────────────────
data "azurerm_client_config" "current" {}

# ── Resource Group ────────────────────────────────────────────────
resource "azurerm_resource_group" "acc" {
  name     = "acc-${var.environment}-rg"
  location = var.location

  tags = local.common_tags
}

# ── Local Values ──────────────────────────────────────────────────
locals {
  common_tags = {
    project     = "acc"
    environment = var.environment
    managed_by  = "terraform"
  }
}
