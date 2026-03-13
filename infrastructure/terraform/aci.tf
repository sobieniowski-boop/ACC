# ═══════════════════════════════════════════════════════════════════
# Azure Container Instances — API + Worker + Web
# ═══════════════════════════════════════════════════════════════════
# Deploys the ACC stack as container groups in ACI.
# For production at scale, consider migrating to Azure Container Apps.
# ═══════════════════════════════════════════════════════════════════

resource "azurerm_container_group" "acc_app" {
  name                = "acc-${var.environment}-app"
  location            = azurerm_resource_group.acc.location
  resource_group_name = azurerm_resource_group.acc.name
  os_type             = "Linux"
  restart_policy      = "Always"

  image_registry_credential {
    server   = azurerm_container_registry.acc.login_server
    username = azurerm_container_registry.acc.admin_username
    password = azurerm_container_registry.acc.admin_password
  }

  # ── API Container ──────────────────────────────────────────────
  container {
    name   = "api"
    image  = "${azurerm_container_registry.acc.login_server}/acc-api:latest"
    cpu    = var.api_cpu
    memory = var.api_memory

    ports {
      port     = 8000
      protocol = "TCP"
    }

    environment_variables = {
      APP_ENV   = var.environment
      REDIS_URL = "redis://${azurerm_redis_cache.acc.hostname}:${azurerm_redis_cache.acc.ssl_port}"
    }

    secure_environment_variables = {
      MSSQL_SERVER   = data.azurerm_key_vault_secret.mssql_server.value
      MSSQL_USER     = data.azurerm_key_vault_secret.mssql_user.value
      MSSQL_PASSWORD = data.azurerm_key_vault_secret.mssql_password.value
      MSSQL_DATABASE = data.azurerm_key_vault_secret.mssql_database.value
      SECRET_KEY     = data.azurerm_key_vault_secret.secret_key.value
    }

    liveness_probe {
      http_get {
        path   = "/api/v1/health"
        port   = 8000
        scheme = "Http"
      }
      initial_delay_seconds = 15
      period_seconds        = 30
      failure_threshold     = 3
    }

    readiness_probe {
      http_get {
        path   = "/api/v1/health"
        port   = 8000
        scheme = "Http"
      }
      initial_delay_seconds = 10
      period_seconds        = 10
      failure_threshold     = 3
    }
  }

  # ── Worker Container ───────────────────────────────────────────
  container {
    name   = "worker"
    image  = "${azurerm_container_registry.acc.login_server}/acc-api:latest"
    cpu    = var.worker_cpu
    memory = var.worker_memory

    commands = [
      "celery", "-A", "app.worker.celery_app", "worker",
      "--loglevel=warning", "--concurrency=4",
      "--queues=default,sync,ai"
    ]

    environment_variables = {
      APP_ENV   = var.environment
      REDIS_URL = "redis://${azurerm_redis_cache.acc.hostname}:${azurerm_redis_cache.acc.ssl_port}"
    }

    secure_environment_variables = {
      MSSQL_SERVER   = data.azurerm_key_vault_secret.mssql_server.value
      MSSQL_USER     = data.azurerm_key_vault_secret.mssql_user.value
      MSSQL_PASSWORD = data.azurerm_key_vault_secret.mssql_password.value
      MSSQL_DATABASE = data.azurerm_key_vault_secret.mssql_database.value
      SECRET_KEY     = data.azurerm_key_vault_secret.secret_key.value
    }
  }

  # ── Web (nginx) Container ──────────────────────────────────────
  container {
    name   = "web"
    image  = "${azurerm_container_registry.acc.login_server}/acc-web:latest"
    cpu    = var.web_cpu
    memory = var.web_memory

    ports {
      port     = 3010
      protocol = "TCP"
    }

    liveness_probe {
      http_get {
        path   = "/"
        port   = 3010
        scheme = "Http"
      }
      initial_delay_seconds = 5
      period_seconds        = 30
      failure_threshold     = 3
    }
  }

  # ── Exposed Ports ──────────────────────────────────────────────
  exposed_port {
    port     = 3010
    protocol = "TCP"
  }

  exposed_port {
    port     = 8000
    protocol = "TCP"
  }

  tags = local.common_tags
}
