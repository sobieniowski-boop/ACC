# ACC — Security Specification

> Version: 2026-03-12 | Classification: INTERNAL — CONFIDENTIAL
> Framework: FastAPI 0.115 | Python 3.12 | Azure SQL

---

## 1. Security Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    INTERNET / CDN                             │
│  (Azure Front Door / Cloudflare)                             │
├─────────────────────────────────────────────────────────────┤
│                    NGINX REVERSE PROXY                        │
│  TLS 1.3 | HSTS | Rate Limiting | CORS                      │
├─────────────────────────────────────────────────────────────┤
│                    FASTAPI APPLICATION                        │
│  JWT Auth → RBAC Middleware → Request Validation              │
│  → Rate Limiter → Circuit Breaker → Handler                  │
├─────────────────────────────────────────────────────────────┤
│                    DATA LAYER                                 │
│  Azure SQL (TLS) | Redis 7 (password) | Fernet Encryption    │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Authentication

### 2.1 JWT Token System

| Property | Value |
|---|---|
| **Algorithm** | HS256 |
| **Access Token Lifetime** | 30 minutes |
| **Refresh Token Lifetime** | 7 days |
| **Secret Key Source** | `JWT_SECRET` env var |
| **Token Location** | `Authorization: Bearer <token>` header |
| **Token Claims** | `sub` (user email), `role`, `exp`, `iat`, `jti` |

### 2.2 Authentication Flow

```
Client → POST /auth/token (email + password)
  → bcrypt hash verification
  → Issue access_token + refresh_token
  → Client stores tokens

Client → GET /api/v1/* (Authorization: Bearer <access_token>)
  → Decode JWT, verify signature + expiry
  → Load user from DB (by email in `sub`)
  → Inject user into request state

Client → POST /auth/refresh (refresh_token)
  → Verify refresh token
  → Issue new access_token + refresh_token
```

### 2.3 Password Security

| Property | Value |
|---|---|
| **Hashing** | bcrypt (passlib CryptContext) |
| **Work Factor** | Default (12 rounds) |
| **Storage** | `hashed_password` column (NVARCHAR(255)) |
| **Change Endpoint** | `POST /auth/change-password` (requires old password) |

---

## 3. Authorization (RBAC)

### 3.1 Role Hierarchy

```
ANALYST  <  OPS  <  CATEGORY_MGR  <  DIRECTOR  <  ADMIN
   1          2         3               4           5
```

Upper roles inherit all permissions of lower roles.

### 3.2 Role Permissions Matrix

| Capability | ANALYST | OPS | CAT_MGR | DIRECTOR | ADMIN |
|---|---|---|---|---|---|
| View dashboards, KPIs, reports | ✅ | ✅ | ✅ | ✅ | ✅ |
| View profit data, orders | ✅ | ✅ | ✅ | ✅ | ✅ |
| Run profitability simulations | ✅ | ✅ | ✅ | ✅ | ✅ |
| Create content tasks | ❌ | ✅ | ✅ | ✅ | ✅ |
| Manage FBA shipment plans | ❌ | ✅ | ✅ | ✅ | ✅ |
| Run import/sync jobs | ❌ | ✅ | ✅ | ✅ | ✅ |
| Update pricing offers | ❌ | ❌ | ✅ | ✅ | ✅ |
| Approve inventory drafts | ❌ | ❌ | ❌ | ✅ | ✅ |
| Review taxonomy predictions | ❌ | ❌ | ❌ | ✅ | ✅ |
| Update inventory settings | ❌ | ❌ | ❌ | ✅ | ✅ |
| Generate AI recommendations | ❌ | ❌ | ❌ | ✅ | ✅ |
| Import financial data | ❌ | ❌ | ❌ | ❌ | ✅ |
| Manual ledger entries | ❌ | ❌ | ❌ | ❌ | ✅ |
| Manage accounts/tax codes | ❌ | ❌ | ❌ | ❌ | ✅ |
| Register new users | ✅* | ✅* | ✅* | ✅* | ✅ |

*User registration requires authentication but no specific role restriction at endpoint level.

### 3.3 Data-Level Access Control

| Mechanism | Description |
|---|---|
| **Marketplace Filtering** | `allowed_marketplaces` on `acc_user` — JSON array of marketplace codes; empty = all |
| **Brand Filtering** | `allowed_brands` on `acc_user` — JSON array of brand names; empty = all |
| **Seller Permissions** | `acc_user_seller_permission` table — per-user, per-seller permission grants |
| **Permission Levels** | viewer, operator, manager, admin (per seller account) |

### 3.4 Auth Dependencies (FastAPI)

```python
# Dependency chain
get_current_user      → decode JWT, load user, check is_active
require_analyst       → get_current_user + role >= ANALYST
require_ops           → get_current_user + role >= OPS
require_role(R)       → get_current_user + role >= R
require_director      → get_current_user + role >= DIRECTOR
require_admin         → get_current_user + role == ADMIN
```

---

## 4. Encryption

### 4.1 Data at Rest

| Data | Encryption | Details |
|---|---|---|
| **Seller Credentials** | Fernet (AES-128-CBC + HMAC-SHA256) | `acc_seller_credential.encrypted_value` |
| **Database** | Azure SQL TDE | Transparent Data Encryption (platform-managed) |
| **Redis** | Password-protected | `REDIS_PASSWORD` env var |

### 4.2 Data in Transit

| Layer | Encryption |
|---|---|
| **Client → API** | TLS 1.2+ (HTTPS enforced) |
| **API → Azure SQL** | TLS encrypted connection (`Encrypt=yes`) |
| **API → Redis** | Password auth (6380 TLS port in Azure, 6379 local) |
| **API → External APIs** | HTTPS (SP-API, DHL, GLS, Ergonode) |

### 4.3 Fernet Key Management

| Property | Value |
|---|---|
| **Key Source** | `FERNET_KEY` env var |
| **Key Format** | URL-safe base64 (32 bytes) |
| **Usage** | Encrypt/decrypt seller API credentials |
| **Rotation** | Manual key rotation with MultiFernet support |

---

## 5. API Security

### 5.1 CORS Configuration

```python
CORSMiddleware(
    allow_origins=["http://localhost:5173", FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 5.2 Rate Limiting

| Mechanism | Details |
|---|---|
| **Implementation** | Custom middleware + Redis-backed counter |
| **Default Limit** | 100 requests/minute per IP |
| **Auth Endpoints** | 10 requests/minute (login, register) |
| **Job Triggers** | 5 requests/minute per endpoint |
| **Header** | `X-RateLimit-Remaining`, `X-RateLimit-Reset` |

### 5.3 Circuit Breaker

| Feature | Details |
|---|---|
| **External API Calls** | Circuit breaker on SP-API, DHL, GLS, Ergonode |
| **States** | CLOSED → OPEN → HALF_OPEN |
| **Failure Threshold** | 5 consecutive failures |
| **Recovery Timeout** | 60 seconds |
| **Content Publish** | Dedicated circuit breaker with `/circuit-breaker/reset` endpoint |

### 5.4 Request Validation

| Layer | Mechanism |
|---|---|
| **Input Validation** | Pydantic v2 models with strict type checking |
| **Path Parameters** | FastAPI path parameter validation |
| **Query Parameters** | Pydantic Query models with constraints |
| **File Uploads** | Size limits, content-type validation |
| **SQL Injection** | Parameterized queries via SQLAlchemy/pyodbc |

### 5.5 Response Security Headers

| Header | Value |
|---|---|
| `Strict-Transport-Security` | `max-age=63072000; includeSubDomains` |
| `X-Content-Type-Options` | `nosniff` |
| `X-Frame-Options` | `DENY` |
| `X-XSS-Protection` | `1; mode=block` |
| `Content-Security-Policy` | `default-src 'self'` |

---

## 6. Infrastructure Security

### 6.1 Docker Security

| Aspect | Configuration |
|---|---|
| **Base Image** | `python:3.12-slim` (minimal attack surface) |
| **Non-Root User** | Container runs as non-root |
| **Secrets** | Via environment variables (not baked into image) |
| **Network** | Internal Docker network for inter-service communication |

### 6.2 Environment Variables (Secrets)

| Variable | Purpose | Sensitivity |
|---|---|---|
| `JWT_SECRET` | JWT signing key | CRITICAL |
| `FERNET_KEY` | Credential encryption key | CRITICAL |
| `DATABASE_URL` | Azure SQL connection string | HIGH |
| `REDIS_PASSWORD` | Redis authentication | HIGH |
| `SP_API_REFRESH_TOKEN` | Amazon SP-API auth | HIGH |
| `SP_API_CLIENT_SECRET` | Amazon SP-API auth | HIGH |
| `ADS_API_CLIENT_SECRET` | Amazon Ads API auth | HIGH |
| `DHL_API_KEY` | DHL tracking API key | MEDIUM |
| `GLS_USERNAME` / `GLS_PASSWORD` | GLS API credentials | MEDIUM |
| `ERGONODE_API_KEY` | Ergonode PIM API key | MEDIUM |
| `SENTRY_DSN` | Sentry error tracking | LOW |

### 6.3 Network Security

| Layer | Control |
|---|---|
| **Azure SQL** | Firewall rules, VNet service endpoints |
| **Redis** | Password auth, private network |
| **Docker Network** | Bridge network, only port 8000 (API) and 5173 (Web) exposed |
| **Outbound** | Whitelist: Amazon APIs, DHL, GLS, ECB, Ergonode, Sentry |

---

## 7. Security Monitoring

### 7.1 Logging

| Aspect | Implementation |
|---|---|
| **Structured Logging** | Python `logging` with JSON formatter |
| **Auth Events** | Login success/failure, token refresh, password change |
| **Access Logs** | Request method, path, status, duration, user email |
| **Error Tracking** | Sentry SDK with user context |
| **OpenTelemetry** | Traces for all requests with span context |

### 7.2 Audit Trail

| Table | Purpose |
|---|---|
| `acc_event_log` | Domain event audit trail (500K+ events) |
| `acc_event_processing_log` | Event handler execution log |
| `acc_event_handler_health` | Handler health/failure metrics |
| `acc_alert` | Security and operational alert log |
| `acc_operator_case` | Manual investigation cases |

### 7.3 Alert Rules

| Alert Type | Trigger |
|---|---|
| Auth failures | > 5 failed logins from same IP in 5 minutes |
| Rate limit breach | IP exceeds rate limit consistently |
| Circuit breaker open | External API circuit breaker opens |
| Data anomaly | Refund anomaly, fee gap, pricing inconsistency |
| Job failure | Scheduled job fails consecutively |

---

## 8. Compliance

### 8.1 GDPR Considerations

| Aspect | Implementation |
|---|---|
| **PII Storage** | Minimal: email addresses, buyer_id (hashed) in serial returner tracking |
| **Encryption** | Seller credentials encrypted (Fernet) |
| **Access Control** | RBAC with marketplace/brand-level filtering |
| **Data Retention** | No automated purge yet — recommended for event logs |

### 8.2 Amazon Marketplace Compliance

| Requirement | Implementation |
|---|---|
| **SP-API Rate Limits** | Respectful rate limiting with exponential backoff |
| **Data Usage** | Only for authorized seller account management |
| **Token Refresh** | Automatic LWA token refresh before expiry |
| **Restricted Data** | PII handling per Amazon Marketplace Developer Agreement |

---

## 9. Known Security Gaps & Recommendations

| # | Gap | Severity | Recommendation |
|---|---|---|---|
| 1 | ~25 routers have no auth | HIGH | Add `require_analyst` to all non-health endpoints |
| 2 | No CSRF protection | MEDIUM | Add CSRF tokens for state-changing operations |
| 3 | No IP allowlisting | MEDIUM | Restrict API access to known IPs for admin endpoints |
| 4 | No MFA | MEDIUM | Add TOTP-based MFA for DIRECTOR/ADMIN roles |
| 5 | No automated secret rotation | MEDIUM | Implement Azure Key Vault with rotation policies |
| 6 | No request body size limit | LOW | Add max content-length middleware (e.g., 10MB) |
| 7 | No audit log retention policy | LOW | Define 90-day retention with archival |
| 8 | JWT in localStorage risk | LOW | Consider HttpOnly cookies for token storage |
