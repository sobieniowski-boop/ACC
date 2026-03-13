# CI-Local test run (API)

This setup does not touch running backfill jobs. It only installs local test dependencies and runs pytest in `apps/api`.

## 1) Minimal dependencies

From repo root:

```powershell
python -m pip install -r apps/api/requirements.txt
python -m pip install pytest pytest-asyncio
```

Optional (nicer output + coverage):

```powershell
python -m pip install pytest-cov
```

## 2) Run only Content Ops tests

```powershell
cd apps/api
python -m pytest tests/test_api_content_ops.py tests/test_content_ops_service_publish.py
```

## 3) Run full API smoke tests

```powershell
cd apps/api
python -m pytest
```

## 4) Quick compile-only sanity (no pytest required)

```powershell
python -m compileall apps/api/app apps/api/tests
```
