# API Architecture — Amazon Command Center (ACC)

> Ostatnia aktualizacja: 2026-03-13

## Przegląd

ACC Backend to aplikacja **FastAPI 0.115+** obsługująca analitykę sprzedaży Amazon dla KADAX.
Łączy się z dwoma bazami danych SQL Server i wystawia REST API dla frontendu React.

**Stos technologiczny:**
- **FastAPI** + ORJSONResponse (async lifespan, Pydantic v2)
- **Azure SQL** (pymssql — TLS 1.2) — baza główna ACC (`acc_*` tabele)
- **On-prem MSSQL** (pyodbc) — baza ERP NetfoxAnalityka (read-only)
- **SQLAlchemy** async engine (pool_size=10, max_overflow=20, pool_pre_ping=True)
- **Redis** — rate limiting, circuit breaker, scheduler lock
- **APScheduler** — in-process scheduler (feature flag)
- **Prometheus** + **Sentry** — monitoring i error tracking

---

## Stos middleware (kolejność wykonania)

```
Request
  │
  ├─ 1. CORSMiddleware          — konfiguracja origins z .env
  ├─ 2. SecurityHeadersMiddleware — X-Content-Type-Options, X-Frame-Options, HSTS
  ├─ 3. CorrelationIdMiddleware  — UUID4 per request, binding do structlog
  ├─ 4. RequestSizeLimitMiddleware — max 10 MB body
  ├─ 5. InternalOnlyMiddleware   — /metrics zablokowany zewnętrznie w prod
  ├─ 6. Prometheus Metrics       — histogram latencji, counter żądań
  ├─ 7. RequestLoggingMiddleware — log method/path/status/duration (pomija /health)
  │
  ▼
 Router → Handler → Response
```

---

## Autentykacja i autoryzacja

### Flow JWT
1. `POST /api/v1/auth/token` — login (email + hasło), zwraca `access_token` + `refresh_token`
2. `POST /api/v1/auth/refresh` — odświeżenie tokenu
3. Każdy chroniony endpoint wymaga `Authorization: Bearer <token>`

### Tokeny
- **Access token**: HS256, krótki TTL
- **Refresh token**: HS256, dłuższy TTL
- Hashowanie haseł: **bcrypt**
- Rate limiting na login: Redis-backed, graceful degradation przy braku Redis

### Role (hierarchia od najniższej)
| Poziom | Rola           | Opis                        |
|--------|----------------|-----------------------------|
| 0      | `analyst`      | Read-only                   |
| 1      | `ops`          | Inventory, orders, shipping |
| 2      | `category_mgr` | Własne kategorie            |
| 3      | `director`     | Pełny dostęp ecommerce      |
| 4      | `admin`        | Pełny dostęp systemowy       |

Dependency `require_role(min_role)` sprawdza hierarchię — wyższe role mają automatycznie uprawnienia niższych.

---

## Baza danych — architektura dual-connection

### `connect_acc()` → Azure SQL (read+write)
- Driver: **pymssql** (TLS 1.2 kompatybilny z Azure SQL)
- Fallback: pyodbc dla lokalnego MSSQL
- Każde połączenie: `SET LOCK_TIMEOUT 30000` (30s anti-deadlock)
- Tabele: `acc_orders`, `acc_order_lines`, `acc_products`, `acc_users`, `acc_finances`, ...

### `connect_netfox()` → On-prem MSSQL (read-only)
- Driver: **pyodbc** (ODBC Driver 17)
- Konwencja: TYLKO odczyt, brak INSERT/UPDATE/DELETE
- Dane ERP: ceny zakupu, stany magazynowe, kontrahenci

### SQLAlchemy Async Engine
- `pool_size=10`, `max_overflow=20`, `pool_recycle=600` (Azure SQL idle timeout)
- `pool_pre_ping=True` — weryfikacja żywotności połączenia
- **READ UNCOMMITTED** isolation na poziomie engine (odpowiednik WITH NOLOCK)

### Konwencje SQL
- Wszystkie SELECT: `WITH (NOLOCK)` hint
- Wszystkie INSERT/UPDATE/DELETE: `SET LOCK_TIMEOUT 30000`
- Brak ORM queries — raw SQL przez pyodbc/pymssql (szczególnie profit_engine.py)

---

## Obsługa błędów

### Standardowy format odpowiedzi błędu
```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Zasób nie został znaleziony",
    "details": null,
    "correlation_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### Hierarchia wyjątków
```
AppException (bazowy)
├── NotFoundError        (404)
├── ValidationError      (422)
├── ConflictError        (409)
├── ForbiddenError       (403)
└── ServiceUnavailableError (503)
```

Globalny handler przechwytuje `AppException` i zwraca ustandaryzowany JSON.
Nieobsłużone wyjątki → `500 {"detail": "Internal server error"}` + log + Sentry.

---

## Standardowe koperty odpowiedzi

### Pojedynczy obiekt — `ApiResponse[T]`
```json
{
  "data": { ... },
  "meta": {
    "timestamp": "2026-03-13T12:00:00+00:00",
    "correlation_id": "..."
  }
}
```

### Lista z paginacją — `PaginatedResponse[T]`
```json
{
  "data": [ ... ],
  "total": 1523,
  "page": 1,
  "page_size": 50,
  "has_next": true,
  "meta": {
    "timestamp": "2026-03-13T12:00:00+00:00",
    "correlation_id": "..."
  }
}
```

---

## Wersjonowanie API

- Prefix: **`/api/v1/`** — wszystkie routery
- Router główny: `app/api/v1/router.py` (~50 sub-routerów)
- Health-check root: `GET /health` (poza prefixem, dla load balancera)
- Statyczny OpenAPI: `openapi.json` + `docs/api-spec.yaml`

---

## Monitoring i observability

### Prometheus (`/metrics`)
- Request latency histogram (per path, method, status)
- Request counter
- Pool status metrics (checked_in, checked_out, overflow)
- Zablokowany z zewnątrz w produkcji (InternalOnlyMiddleware)

### Sentry
- FastAPI + Starlette integration
- `traces_sample_rate=0.1` w produkcji
- `send_default_pii=False`

### Health checks
| Endpoint                     | Opis                          | Auth    |
|------------------------------|-------------------------------|---------|
| `GET /health`                | LB probe, zawsze 200          | —       |
| `GET /api/v1/health/deep`   | Azure SQL + Redis + SP-API    | Wymagany |
| `GET /api/v1/health/netfox-sessions` | Sesje ERP            | Wymagany |
| `GET /api/v1/health/order-sync`     | Status synchronizacji | Wymagany |
| `GET /api/v1/health/sp-api-usage`   | Limity SP-API         | Wymagany |

### Structured logging
- **structlog** + stdlib bridge
- JSON w produkcji, kolorowy console w dev
- Correlation ID binding automatyczny (CorrelationIdMiddleware)

---

## Development

### Seed data
```bash
cd apps/api
python -m scripts.seed_dev_data
```
Tworzy użytkownika `dev@acc.local` (admin) i przykładowe marketplace (DE, PL, FR).
Bezpieczny do uruchomienia wielokrotnie (idempotentny).

### Export OpenAPI
```bash
cd apps/api
python -m scripts.export_openapi
```
Generuje `openapi.json` i `docs/api-spec.yaml`.

### Testy
```bash
cd apps/api
pytest
```
66 plików testowych, `asyncio_mode = "auto"`.

### Migracje
```bash
cd apps/api
alembic upgrade head
```
39 plików migracji w `migrations/versions/`.

---

## Struktura katalogów (kluczowe pliki)

```
apps/api/
├── app/
│   ├── main.py                    — FastAPI app, lifespan, middleware
│   ├── api/v1/
│   │   ├── router.py             — 50+ sub-routerów
│   │   ├── auth.py               — login, refresh, register
│   │   ├── profit_v2.py          — profit dashboard endpoints
│   │   └── routes_health.py      — health checks
│   ├── core/
│   │   ├── config.py             — pydantic-settings z .env
│   │   ├── database.py           — SQLAlchemy async engine
│   │   ├── db_connection.py      — connect_acc() / connect_netfox()
│   │   ├── security.py           — JWT, bcrypt, RBAC
│   │   ├── exceptions.py         — standardowe wyjątki + handler
│   │   └── metrics.py            — Prometheus setup
│   ├── platform/middleware/
│   │   ├── __init__.py           — CorrelationIdMiddleware
│   │   └── request_logging.py    — RequestLoggingMiddleware
│   ├── schemas/
│   │   ├── common.py             — ApiResponse, PaginatedResponse
│   │   └── ...                   — domain-specific schemas
│   ├── models/                   — SQLAlchemy models
│   ├── services/                 — logika biznesowa
│   └── connectors/               — Amazon SP-API, Ads API
├── scripts/
│   ├── seed_dev_data.py          — dane deweloperskie
│   └── export_openapi.py         — eksport specyfikacji
├── migrations/                   — Alembic
├── tests/                        — pytest
└── openapi.json                  — statyczna specyfikacja
```
