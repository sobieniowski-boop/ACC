# Content Ops Studio - P0 API Payloads (JSON examples)

Base path: `/api/v1/content`

## 1) GET `/tasks`

Query example:
`/api/v1/content/tasks?status=open&owner=anna&marketplace_id=A1PA6795UKMFR9&type=refresh_content&priority=p1&sku_search=KAD-123&page=1&page_size=50`

Response `200`:
```json
{
  "total": 2,
  "page": 1,
  "page_size": 50,
  "pages": 1,
  "items": [
    {
      "id": "f053f170-65f2-4c89-9252-86f7f58f7428",
      "type": "refresh_content",
      "sku": "KAD-123",
      "asin": "B0ABC12345",
      "marketplace_id": "A1PA6795UKMFR9",
      "priority": "p1",
      "owner": "anna",
      "due_date": "2026-03-06T12:00:00Z",
      "status": "open",
      "tags_json": { "season": "spring", "brand": "KADAX" },
      "title": "Refresh bullets after returns spike",
      "note": "Focus on assembly clarity",
      "source_page": "content_dashboard",
      "created_by": "director@kadax.com",
      "created_at": "2026-03-02T09:10:00Z",
      "updated_at": "2026-03-02T09:10:00Z"
    }
  ]
}
```

## 2) POST `/tasks`

Request:
```json
{
  "type": "fix_policy",
  "sku": "KAD-123",
  "asin": "B0ABC12345",
  "marketplace_id": "A1PA6795UKMFR9",
  "priority": "p0",
  "owner": "anna",
  "due_date": "2026-03-05T15:00:00Z",
  "tags_json": { "risk": "critical" },
  "title": "Remove prohibited medical claim",
  "note": "Claim found in DE title",
  "source_page": "compliance_center"
}
```

Response `201`:
```json
{
  "id": "8f8fd834-8b99-4d8b-9c9f-0ea5fce5ca5a",
  "type": "fix_policy",
  "sku": "KAD-123",
  "asin": "B0ABC12345",
  "marketplace_id": "A1PA6795UKMFR9",
  "priority": "p0",
  "owner": "anna",
  "due_date": "2026-03-05T15:00:00Z",
  "status": "open",
  "tags_json": { "risk": "critical" },
  "title": "Remove prohibited medical claim",
  "note": "Claim found in DE title",
  "source_page": "compliance_center",
  "created_by": "director@kadax.com",
  "created_at": "2026-03-02T10:00:00Z",
  "updated_at": "2026-03-02T10:00:00Z"
}
```

## 3) PATCH `/tasks/{task_id}`

Request:
```json
{
  "status": "investigating",
  "owner": "marta",
  "priority": "p1",
  "note": "Waiting for legal review"
}
```

Response `200`:
```json
{
  "id": "8f8fd834-8b99-4d8b-9c9f-0ea5fce5ca5a",
  "type": "fix_policy",
  "sku": "KAD-123",
  "asin": "B0ABC12345",
  "marketplace_id": "A1PA6795UKMFR9",
  "priority": "p1",
  "owner": "marta",
  "due_date": "2026-03-05T15:00:00Z",
  "status": "investigating",
  "tags_json": { "risk": "critical" },
  "title": "Remove prohibited medical claim",
  "note": "Waiting for legal review",
  "source_page": "compliance_center",
  "created_by": "director@kadax.com",
  "created_at": "2026-03-02T10:00:00Z",
  "updated_at": "2026-03-02T10:20:00Z"
}
```

## 4) GET `/{sku}/{marketplace_id}/versions`

Response `200`:
```json
{
  "sku": "KAD-123",
  "marketplace_id": "A1PA6795UKMFR9",
  "items": [
    {
      "id": "57ca3aca-4f26-47e4-8f01-72be654b52be",
      "sku": "KAD-123",
      "asin": "B0ABC12345",
      "marketplace_id": "A1PA6795UKMFR9",
      "version_no": 4,
      "status": "draft",
      "fields": {
        "title": "KADAX Pflanzkasten mit Griff",
        "bullets": ["Robustes Material", "Einfache Reinigung"],
        "description": "Produktbeschreibung...",
        "keywords": "pflanzkasten balkon garten",
        "special_features": ["UV resistant"],
        "attributes_json": { "material": "kunststoff" },
        "aplus_json": {},
        "compliance_notes": "No superlatives"
      },
      "created_by": "anna",
      "created_at": "2026-03-02T10:25:00Z",
      "approved_by": null,
      "approved_at": null,
      "published_at": null,
      "parent_version_id": "2f5f8a6b-5132-4434-bab3-6caed5d58c88"
    }
  ]
}
```

## 5) POST `/{sku}/{marketplace_id}/versions`

Request:
```json
{
  "asin": "B0ABC12345",
  "base_version_id": "2f5f8a6b-5132-4434-bab3-6caed5d58c88",
  "fields": {
    "title": "KADAX Pflanzkasten mit Griff",
    "bullets": ["Robustes Material", "Einfache Reinigung"],
    "description": "Produktbeschreibung...",
    "keywords": "pflanzkasten balkon garten",
    "special_features": ["UV resistant"],
    "attributes_json": { "material": "kunststoff" },
    "aplus_json": {},
    "compliance_notes": "Draft initialized"
  }
}
```

Response `201`: `ContentVersionItem` (same shape as in section 4).

## 6) PUT `/versions/{version_id}`

Request:
```json
{
  "fields": {
    "title": "KADAX Pflanzkasten mit ergonomischem Griff",
    "bullets": ["Robustes Material", "Einfache Reinigung", "Frostbeständig"],
    "description": "Aktualisowana treść...",
    "keywords": "pflanzkasten balkon garten frostfest",
    "special_features": ["UV resistant", "Frost resistant"],
    "attributes_json": { "material": "kunststoff", "capacity": "12L" },
    "aplus_json": {},
    "compliance_notes": "Checked with policy checker"
  }
}
```

Response `200`: `ContentVersionItem`.

## 7) POST `/versions/{version_id}/submit-review`

Request body: empty

Response `200`: `ContentVersionItem` with `"status": "review"`.

## 8) POST `/versions/{version_id}/approve`

Request body: empty

Response `200`: `ContentVersionItem` with `"status": "approved"` and `approved_at`.

## 9) POST `/policy/check`

Request:
```json
{
  "version_id": "57ca3aca-4f26-47e4-8f01-72be654b52be"
}
```

Response `200`:
```json
{
  "version_id": "57ca3aca-4f26-47e4-8f01-72be654b52be",
  "passed": false,
  "critical_count": 1,
  "major_count": 1,
  "minor_count": 0,
  "findings": [
    {
      "rule_id": "4f2b0f5d-87fb-4d3f-9c10-9af0352e8f4e",
      "rule_name": "medical_claim",
      "severity": "critical",
      "field": "title",
      "message": "Medical claim detected",
      "snippet": "heilt Schmerzen"
    }
  ],
  "checked_at": "2026-03-02T10:40:00Z",
  "checker_version": "policy-lint-v1"
}
```

## 10) GET `/policy/rules`

Response `200`:
```json
[
  {
    "id": "4f2b0f5d-87fb-4d3f-9c10-9af0352e8f4e",
    "name": "medical_claim",
    "pattern": "(heilt|cures|100% cure)",
    "severity": "critical",
    "applies_to_json": { "fields": ["title", "bullets", "description"] },
    "is_active": true,
    "created_by": "admin",
    "created_at": "2026-02-20T08:00:00Z"
  }
]
```

## 11) PUT `/policy/rules`

Request:
```json
{
  "rules": [
    {
      "id": "4f2b0f5d-87fb-4d3f-9c10-9af0352e8f4e",
      "name": "medical_claim",
      "pattern": "(heilt|cures|100% cure)",
      "severity": "critical",
      "applies_to_json": { "fields": ["title", "bullets", "description"] },
      "is_active": true
    },
    {
      "name": "superlative_best",
      "pattern": "\\bbest\\b",
      "severity": "major",
      "applies_to_json": { "fields": ["title", "bullets"] },
      "is_active": true
    }
  ]
}
```

Response `200`: array `PolicyRuleItem`.

## 12) POST `/assets/upload`

Request:
```json
{
  "filename": "manual_de.pdf",
  "mime": "application/pdf",
  "content_base64": "JVBERi0xLjQKJ....",
  "metadata_json": {
    "tags": ["manual", "de"],
    "language": "de_DE"
  }
}
```

Response `201`:
```json
{
  "id": "51fe43ed-688b-42a5-97bc-90f6775dbb5f",
  "filename": "manual_de.pdf",
  "mime": "application/pdf",
  "content_hash": "sha256:0f9dbf7d...",
  "storage_path": "content-assets/2026/03/manual_de.pdf",
  "metadata_json": {
    "tags": ["manual", "de"],
    "language": "de_DE"
  },
  "status": "approved",
  "uploaded_by": "anna",
  "uploaded_at": "2026-03-02T10:50:00Z"
}
```

## 13) GET `/assets`

Query example:
`/api/v1/content/assets?sku=KAD-123&tag=manual&role=manual&status=approved&page=1&page_size=50`

Response `200`:
```json
{
  "total": 1,
  "page": 1,
  "page_size": 50,
  "pages": 1,
  "items": [
    {
      "id": "51fe43ed-688b-42a5-97bc-90f6775dbb5f",
      "filename": "manual_de.pdf",
      "mime": "application/pdf",
      "content_hash": "sha256:0f9dbf7d...",
      "storage_path": "content-assets/2026/03/manual_de.pdf",
      "metadata_json": { "tags": ["manual", "de"] },
      "status": "approved",
      "uploaded_by": "anna",
      "uploaded_at": "2026-03-02T10:50:00Z"
    }
  ]
}
```

## 14) POST `/assets/{asset_id}/link`

Request:
```json
{
  "sku": "KAD-123",
  "asin": "B0ABC12345",
  "marketplace_id": "A1PA6795UKMFR9",
  "role": "manual",
  "status": "approved"
}
```

Response `201`:
```json
{
  "id": "cb12df11-8bc3-4b96-998f-c9cdf4f0f8d7",
  "asset_id": "51fe43ed-688b-42a5-97bc-90f6775dbb5f",
  "sku": "KAD-123",
  "asin": "B0ABC12345",
  "marketplace_id": "A1PA6795UKMFR9",
  "role": "manual",
  "status": "approved",
  "created_at": "2026-03-02T10:55:00Z"
}
```

## 15) POST `/publish/package`

Request:
```json
{
  "marketplaces": ["DE", "FR", "IT"],
  "selection": "approved",
  "format": "xlsx",
  "sku_filter": ["KAD-123", "KAD-456"]
}
```

Response `200`:
```json
{
  "id": "fb59a239-1699-4dda-a0a8-2104f3d2f4de",
  "job_type": "publish_package",
  "marketplaces": ["DE", "FR", "IT"],
  "selection_mode": "approved",
  "status": "completed",
  "progress_pct": 100,
  "log_json": { "items_count": 42 },
  "artifact_url": "/api/v1/content/publish/jobs/fb59a239-1699-4dda-a0a8-2104f3d2f4de/download",
  "created_by": "anna",
  "created_at": "2026-03-02T11:00:00Z",
  "finished_at": "2026-03-02T11:00:07Z"
}
```

## 16) GET `/publish/jobs`

Response `200`:
```json
{
  "total": 3,
  "page": 1,
  "page_size": 50,
  "pages": 1,
  "items": [
    {
      "id": "fb59a239-1699-4dda-a0a8-2104f3d2f4de",
      "job_type": "publish_package",
      "marketplaces": ["DE", "FR", "IT"],
      "selection_mode": "approved",
      "status": "completed",
      "progress_pct": 100,
      "log_json": { "items_count": 42 },
      "artifact_url": "/api/v1/content/publish/jobs/fb59a239-1699-4dda-a0a8-2104f3d2f4de/download",
      "created_by": "anna",
      "created_at": "2026-03-02T11:00:00Z",
      "finished_at": "2026-03-02T11:00:07Z"
    }
  ]
}
```

## 16a) POST `/publish/push`

Request (preview):
```json
{
  "marketplaces": ["DE", "FR", "IT"],
  "selection": "approved",
  "sku_filter": ["KAD-123"],
  "mode": "preview"
}
```

Response `200`:
```json
{
  "id": "9be8f4ad-4d2f-4af9-8bb9-e2d3ccddce7f",
  "job_type": "publish_push",
  "marketplaces": ["DE", "FR", "IT"],
  "selection_mode": "approved",
  "status": "completed",
  "progress_pct": 100,
  "log_json": {
    "mode": "preview",
    "total_candidates": 18,
    "per_marketplace": {
      "DE": { "status": "preview_ready", "items": 8 },
      "FR": { "status": "preview_ready", "items": 6 },
      "IT": { "status": "preview_ready", "items": 4 }
    }
  },
  "artifact_url": null,
  "created_by": "system",
  "created_at": "2026-03-02T15:12:00Z",
  "finished_at": "2026-03-02T15:12:01Z"
}
```

Request (confirm):
```json
{
  "marketplaces": ["DE", "FR", "IT"],
  "selection": "approved",
  "mode": "confirm"
}
```

Response `200`:
```json
{
  "id": "a5e20e64-2169-4b2e-b036-087d85774ff0",
  "job_type": "publish_push",
  "marketplaces": ["DE", "FR", "IT"],
  "selection_mode": "approved",
  "status": "partial",
  "progress_pct": 100,
  "log_json": {
    "mode": "confirm",
    "success_count": 2,
    "failed_count": 1,
    "per_marketplace": {
      "DE": { "status": "submitted", "items": 8 },
      "FR": { "status": "submitted", "items": 6 },
      "IT": { "status": "failed", "reason": "bridge_http_404", "items": 4 }
    }
  },
  "artifact_url": null,
  "created_by": "system",
  "created_at": "2026-03-02T15:15:00Z",
  "finished_at": "2026-03-02T15:15:08Z"
}
```

## 17) GET `/{sku}/diff`

Query example:
`/api/v1/content/KAD-123/diff?main=DE&target=FR&version_main=57ca...&version_target=72ad...`

Response `200`:
```json
{
  "sku": "KAD-123",
  "main_market": "DE",
  "target_market": "FR",
  "version_main": "57ca3aca-4f26-47e4-8f01-72be654b52be",
  "version_target": "72ad6e9c-e693-423a-8db3-9cd171e7400c",
  "fields": [
    {
      "field": "title",
      "main_value": "KADAX Pflanzkasten mit Griff",
      "target_value": "KADAX Bac a plantes avec poignee",
      "change_type": "changed"
    },
    {
      "field": "keywords",
      "main_value": "pflanzkasten balkon garten",
      "target_value": null,
      "change_type": "removed"
    }
  ],
  "created_at": "2026-03-02T11:10:00Z"
}
```

## 18) POST `/{sku}/sync`

Request:
```json
{
  "fields": ["title", "bullets", "description", "keywords"],
  "from_market": "DE",
  "to_markets": ["FR", "IT", "ES"],
  "overwrite_mode": "missing_only"
}
```

Response `200`:
```json
{
  "sku": "KAD-123",
  "from_market": "DE",
  "to_markets": ["FR", "IT", "ES"],
  "drafts_created": 3,
  "skipped": 0,
  "warnings": []
}
```

## 19) POST `/ai/generate`

Request:
```json
{
  "sku": "KAD-123",
  "marketplace_id": "DE",
  "mode": "improve",
  "constraints_json": {
    "goal": "seo",
    "forbidden_claims": true,
    "max_title_len": 200
  },
  "source_market": "PL",
  "fields": ["title", "bullets", "description", "keywords"],
  "model": "gpt-5.2"
}
```

## 20) POST `/onboard/preflight`

Request:
```json
{
  "sku_list": ["KAD-123", "KAD-456"],
  "main_market": "DE",
  "target_markets": ["FR", "IT", "ES", "NL"],
  "auto_create_tasks": true
}
```

Response `200`:
```json
{
  "main_market": "DE",
  "target_markets": ["FR", "IT", "ES", "NL"],
  "items": [
    {
      "sku": "KAD-123",
      "asin": "B0ABC12345",
      "ean": "5903699455531",
      "brand": "KADAX",
      "title": "KADAX Pflanzkasten",
      "pim_score": 100,
      "family_coverage_pct": 87.5,
      "blockers": [],
      "warnings": [
        "family_coverage_partial: 87.5%",
        "restrictions_not_checked: connect ProductOnboard/SP-API bridge for gating"
      ],
      "recommended_actions": [
        "Podlacz restrictions pre-check (ProductOnboard lub SP-API bridge) przed push."
      ],
      "tasks_created": []
    }
  ],
  "generated_at": "2026-03-02T14:25:00Z"
}
```

## 21) POST `/qa/verify`

Request:
```json
{
  "sku": "KAD-123",
  "marketplace_id": "DE",
  "target_language": "de_DE",
  "pim_facts_json": {
    "brand": "KADAX",
    "color": "anthrazit"
  },
  "content": {
    "title": "KADAX Pflanzkasten",
    "bullets": ["Robust", "Leicht", "Frostfest"],
    "description": "Ideal fur Balkon und Garten.",
    "keywords": "pflanzkasten balkon garten",
    "special_features": [],
    "attributes_json": {},
    "aplus_json": {},
    "compliance_notes": null
  }
}
```

## 22) GET `/onboard/catalog/search-by-ean`

Query:
`/api/v1/content/onboard/catalog/search-by-ean?ean=5903699455531&marketplace=DE`

Response `200`:
```json
{
  "query": "5903699455531",
  "marketplace": "DE",
  "total": 1,
  "matches": [
    {
      "asin": "B0ABC12345",
      "title": "KADAX Pflanzkasten",
      "brand": "KADAX",
      "product_type": "HOME",
      "image_url": "https://images.example.com/asin.jpg"
    }
  ]
}
```

## 23) GET `/onboard/restrictions/check`

Query:
`/api/v1/content/onboard/restrictions/check?asin=B0ABC12345&marketplace=DE`

Response `200`:
```json
{
  "asin": "B0ABC12345",
  "marketplace": "DE",
  "can_list": false,
  "requires_approval": true,
  "reasons": [
    "APPROVAL_REQUIRED: Listing requires approval for this category."
  ]
}
```

## 24) GET `/publish/product-type-mappings`

Response `200`:
```json
[
  {
    "id": "77777777-7777-7777-7777-777777777777",
    "marketplace_id": "A1PA6795UKMFR9",
    "brand": "KADAX",
    "category": "HOME",
    "subcategory": "PLANTERS",
    "product_type": "PLANTER",
    "required_attrs": ["material", "size_name"],
    "priority": 10,
    "is_active": true
  }
]
```

## 25) PUT `/publish/product-type-mappings`

Request:
```json
{
  "rules": [
    {
      "marketplace_id": "A1PA6795UKMFR9",
      "brand": "KADAX",
      "category": "HOME",
      "subcategory": "PLANTERS",
      "product_type": "PLANTER",
      "required_attrs": ["material", "size_name"],
      "priority": 10,
      "is_active": true
    }
  ]
}
```

Response `200`: array `ContentProductTypeMapRule`.

## 26) GET `/publish/product-type-definitions`

Query:
`/api/v1/content/publish/product-type-definitions?marketplace=DE&product_type=PLANTER`

Response `200`:
```json
[
  {
    "id": "99999999-9999-9999-9999-999999999999",
    "marketplace_id": "A1PA6795UKMFR9",
    "marketplace_code": "DE",
    "product_type": "PLANTER",
    "requirements_json": { "required": ["material", "size_name"] },
    "required_attrs": ["material", "size_name"],
    "refreshed_at": "2026-03-02T18:30:00Z",
    "source": "sp_api_definitions"
  }
]
```

## 27) POST `/publish/product-type-definitions/refresh`

Request:
```json
{
  "marketplace": "DE",
  "product_type": "PLANTER",
  "force_refresh": true
}
```

Response `200`: `ContentProductTypeDefinitionItem`.

## 28) POST `/tasks/bulk-update`

Request:
```json
{
  "task_ids": [
    "11111111-1111-1111-1111-111111111111",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
  ],
  "status": "investigating"
}
```

Response `200`:
```json
{
  "updated_count": 2,
  "task_ids": [
    "11111111-1111-1111-1111-111111111111",
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
  ]
}
```

## 29) GET `/compliance/queue`

Query:
`/api/v1/content/compliance/queue?severity=critical&page=1&page_size=50`

## 30) GET `/impact`

Query:
`/api/v1/content/impact?sku=KAD-123&marketplace=DE&range=14`

## 31) GET `/data-quality`

Response contains cards + top lists missing title/bullets/description for content versions.

## 32) GET `/publish/attribute-mappings`

Response `200`:
```json
[
  {
    "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "marketplace_id": "A1PA6795UKMFR9",
    "product_type": "PLANTER",
    "source_field": "fields.title",
    "target_attribute": "item_name",
    "transform": "identity",
    "priority": 100,
    "is_active": true
  }
]
```

## 33) PUT `/publish/attribute-mappings`

Request:
```json
{
  "rules": [
    {
      "marketplace_id": "A1PA6795UKMFR9",
      "product_type": "PLANTER",
      "source_field": "fields.attributes_json.material",
      "target_attribute": "material",
      "transform": "trim",
      "priority": 20,
      "is_active": true
    }
  ]
}
```

## 34) GET `/publish/coverage`

Query:
`/api/v1/content/publish/coverage?marketplaces=DE,FR&selection=approved`

Response includes coverage per marketplace/category/product_type and `missing_required_top`.

## 35) Publish blocker behavior

`POST /publish/push` in `mode=confirm` now blocks listing push when PTD-required attrs are missing:
- no native submit for blocked SKU
- no bridge fallback for preflight-blocked SKU
- job log contains `preflight_blocker_*` reason details.

Current blocker classes:
- `preflight_blocker_missing_required` - required Amazon attrs are missing after attribute mapping.
- `preflight_blocker_ptd_missing_definition` - no PTD definition cached for `marketplace + product_type`.
- `preflight_blocker_ptd_empty_required_attrs` - PTD exists but required attrs payload is empty (non-deterministic push is blocked).

Queue behavior for `mode=confirm`:
- API returns accepted response with job state `queued`.
- Processing is picked up by scheduler queue worker (durable DB-backed execution).
- Status transitions: `queued -> running -> completed | partial | failed`.

Response `200`:
```json
{
  "status": "needs_revision",
  "score": 87.0,
  "critical_count": 0,
  "major_count": 1,
  "minor_count": 1,
  "findings": [
    {
      "category": "accuracy",
      "severity": "major",
      "field": "title",
      "message": "Title shorter than 30 chars.",
      "suggestion": "Rozwin tytul o kluczowe cechy i brand."
    }
  ],
  "checks_json": {
    "lengths": {
      "title": 18,
      "bullets": 3,
      "description": 31,
      "keywords": 26
    },
    "target_language": "de_de",
    "pim_brand_used": true,
    "pim_color_used": false
  },
  "checked_at": "2026-03-02T14:31:00Z"
}
```

Response `200`:
```json
{
  "sku": "KAD-123",
  "marketplace_id": "DE",
  "mode": "improve",
  "model": "gpt-5.2",
  "cache_hit": false,
  "policy_flags": [],
  "output": {
    "title": "KADAX Pflanzkasten fuer Balkon und Garten",
    "bullets": ["Robust und langlebig", "Leichte Reinigung"],
    "description": "Optimized DE description...",
    "keywords": "pflanzkasten balkon garten kadax",
    "special_features": ["UV resistant"],
    "attributes_json": { "material": "kunststoff" },
    "aplus_json": {},
    "compliance_notes": "No policy violation detected"
  },
  "generated_at": "2026-03-02T11:20:00Z"
}
```

## Error scaffold behavior

Current scaffold returns `501` for not yet implemented service logic:
```json
{
  "detail": "create_content_task not implemented"
}
```
