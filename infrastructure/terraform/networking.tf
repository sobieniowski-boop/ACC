# ═══════════════════════════════════════════════════════════════════
# Networking — VNET, Subnets, NSG
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_virtual_network" "acc" {
  name                = "acc-${var.environment}-vnet"
  location            = azurerm_resource_group.acc.location
  resource_group_name = azurerm_resource_group.acc.name
  address_space       = [var.vnet_address_space]

  tags = local.common_tags
}

# ── ACI Subnet ───────────────────────────────────────────────────
resource "azurerm_subnet" "aci" {
  name                 = "aci-subnet"
  resource_group_name  = azurerm_resource_group.acc.name
  virtual_network_name = azurerm_virtual_network.acc.name
  address_prefixes     = [var.subnet_aci_prefix]

  delegation {
    name = "aci-delegation"
    service_delegation {
      name    = "Microsoft.ContainerInstance/containerGroups"
      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
    }
  }
}

# ── Private Endpoints Subnet ─────────────────────────────────────
resource "azurerm_subnet" "private" {
  name                 = "private-endpoints-subnet"
  resource_group_name  = azurerm_resource_group.acc.name
  virtual_network_name = azurerm_virtual_network.acc.name
  address_prefixes     = [var.subnet_private_prefix]
}

# ── Network Security Group — ACI ─────────────────────────────────
resource "azurerm_network_security_group" "aci" {
  name                = "acc-${var.environment}-aci-nsg"
  location            = azurerm_resource_group.acc.location
  resource_group_name = azurerm_resource_group.acc.name

  # Allow inbound HTTP(S) to web container
  security_rule {
    name                       = "allow-http"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "3010"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  # Allow inbound to API (restrict in production)
  security_rule {
    name                       = "allow-api"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8000"
    source_address_prefix      = var.subnet_aci_prefix
    destination_address_prefix = "*"
  }

  # Deny all other inbound
  security_rule {
    name                       = "deny-all-inbound"
    priority                   = 4096
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = local.common_tags
}

resource "azurerm_subnet_network_security_group_association" "aci" {
  subnet_id                 = azurerm_subnet.aci.id
  network_security_group_id = azurerm_network_security_group.aci.id
}
