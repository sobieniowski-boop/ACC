"""Pydantic schemas for Content Ops Studio (MVP scaffold)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


TaskType = Literal["create_listing", "refresh_content", "fix_policy", "expand_marketplaces"]
TaskStatus = Literal["open", "investigating", "resolved"]
TaskPriority = Literal["p0", "p1", "p2", "p3"]
VersionStatus = Literal["draft", "review", "approved", "published"]
PolicySeverity = Literal["critical", "major", "minor"]
AssetRole = Literal["main_image", "manual", "cert", "aplus", "lifestyle", "infographic", "other"]
AssetStatus = Literal["approved", "deprecated", "draft"]
PublishSelection = Literal["approved", "draft"]
PublishFormat = Literal["xlsx", "csv"]
PublishPushMode = Literal["preview", "confirm"]
AIMode = Literal["new_listing", "improve", "localize"]
QAVerifyStatus = Literal["passed", "needs_revision", "rejected"]


class ContentTaskCreate(BaseModel):
    type: TaskType
    sku: str
    asin: Optional[str] = None
    marketplace_id: Optional[str] = None
    priority: TaskPriority = "p1"
    owner: Optional[str] = None
    due_date: Optional[datetime] = None
    tags_json: dict[str, Any] = Field(default_factory=dict)
    title: Optional[str] = None
    note: Optional[str] = None
    source_page: str = "content_dashboard"


class ContentTaskUpdate(BaseModel):
    status: Optional[TaskStatus] = None
    owner: Optional[str] = None
    priority: Optional[TaskPriority] = None
    due_date: Optional[datetime] = None
    title: Optional[str] = None
    note: Optional[str] = None


class ContentTaskBulkUpdateRequest(BaseModel):
    task_ids: list[str] = Field(default_factory=list)
    status: Optional[TaskStatus] = None
    owner: Optional[str] = None
    priority: Optional[TaskPriority] = None


class ContentTaskBulkUpdateResponse(BaseModel):
    updated_count: int
    task_ids: list[str] = Field(default_factory=list)


class ContentTaskItem(BaseModel):
    id: str
    type: TaskType
    sku: str
    asin: Optional[str] = None
    marketplace_id: Optional[str] = None
    priority: TaskPriority
    owner: Optional[str] = None
    due_date: Optional[datetime] = None
    status: TaskStatus
    tags_json: dict[str, Any] = Field(default_factory=dict)
    title: Optional[str] = None
    note: Optional[str] = None
    source_page: Optional[str] = None
    created_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class ContentTaskListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ContentTaskItem]


class ContentFields(BaseModel):
    title: Optional[str] = None
    bullets: list[str] = Field(default_factory=list)
    description: Optional[str] = None
    keywords: Optional[str] = None
    special_features: list[str] = Field(default_factory=list)
    attributes_json: dict[str, Any] = Field(default_factory=dict)
    aplus_json: dict[str, Any] = Field(default_factory=dict)
    compliance_notes: Optional[str] = None


class ContentVersionCreate(BaseModel):
    asin: Optional[str] = None
    base_version_id: Optional[str] = None
    fields: Optional[ContentFields] = None


class ContentVersionUpdate(BaseModel):
    fields: ContentFields


class ContentVersionItem(BaseModel):
    id: str
    sku: str
    asin: Optional[str] = None
    marketplace_id: str
    version_no: int
    status: VersionStatus
    fields: ContentFields
    created_by: Optional[str] = None
    created_at: datetime
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    parent_version_id: Optional[str] = None


class ContentVersionListResponse(BaseModel):
    sku: str
    marketplace_id: str
    items: list[ContentVersionItem]


class PolicyRuleUpsert(BaseModel):
    id: Optional[str] = None
    name: str
    pattern: str
    severity: PolicySeverity
    applies_to_json: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True


class PolicyRuleItem(BaseModel):
    id: str
    name: str
    pattern: str
    severity: PolicySeverity
    applies_to_json: dict[str, Any] = Field(default_factory=dict)
    is_active: bool
    created_by: Optional[str] = None
    created_at: datetime


class PolicyRulesUpsertRequest(BaseModel):
    rules: list[PolicyRuleUpsert]


class PolicyFinding(BaseModel):
    rule_id: Optional[str] = None
    rule_name: Optional[str] = None
    severity: PolicySeverity
    field: str
    message: str
    snippet: Optional[str] = None


class PolicyCheckRequest(BaseModel):
    version_id: str


class PolicyCheckResponse(BaseModel):
    version_id: str
    passed: bool
    critical_count: int
    major_count: int
    minor_count: int
    findings: list[PolicyFinding] = Field(default_factory=list)
    checked_at: datetime
    checker_version: str


class AssetUploadRequest(BaseModel):
    filename: str
    mime: str
    content_base64: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class AssetItem(BaseModel):
    id: str
    filename: str
    mime: str
    content_hash: str
    storage_path: str
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    status: AssetStatus
    uploaded_by: Optional[str] = None
    uploaded_at: datetime


class AssetListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[AssetItem]


class AssetLinkCreate(BaseModel):
    sku: str
    asin: Optional[str] = None
    marketplace_id: Optional[str] = None
    role: AssetRole
    status: AssetStatus = "approved"


class AssetLinkItem(BaseModel):
    id: str
    asset_id: str
    sku: str
    asin: Optional[str] = None
    marketplace_id: Optional[str] = None
    role: AssetRole
    status: AssetStatus
    created_at: datetime


class PublishPackageRequest(BaseModel):
    marketplaces: list[str] = Field(default_factory=list)
    selection: PublishSelection = "approved"
    format: PublishFormat = "xlsx"
    sku_filter: list[str] = Field(default_factory=list)


class PublishPushRequest(BaseModel):
    marketplaces: list[str] = Field(default_factory=list)
    selection: PublishSelection = "approved"
    sku_filter: list[str] = Field(default_factory=list)
    version_ids: list[str] = Field(default_factory=list)
    mode: PublishPushMode = "preview"
    idempotency_key: Optional[str] = None


class PublishPushAcceptedResponse(BaseModel):
    job: PublishJobItem
    queued: bool = True
    detail: str


class PublishJobItem(BaseModel):
    id: str
    job_type: str
    marketplaces: list[str] = Field(default_factory=list)
    selection_mode: PublishSelection
    status: str
    progress_pct: float = 0
    log_json: dict[str, Any] = Field(default_factory=dict)
    artifact_url: Optional[str] = None
    created_by: Optional[str] = None
    created_at: datetime
    finished_at: Optional[datetime] = None


class PublishJobsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[PublishJobItem]


class ContentDiffField(BaseModel):
    field: str
    main_value: Any = None
    target_value: Any = None
    change_type: Literal["added", "removed", "changed", "same"]


class ContentDiffResponse(BaseModel):
    sku: str
    main_market: str
    target_market: str
    version_main: Optional[str] = None
    version_target: Optional[str] = None
    fields: list[ContentDiffField] = Field(default_factory=list)
    created_at: datetime


class ContentSyncRequest(BaseModel):
    fields: list[str] = Field(default_factory=list)
    from_market: str
    to_markets: list[str] = Field(default_factory=list)
    overwrite_mode: Literal["missing_only", "force"] = "missing_only"


class ContentSyncResponse(BaseModel):
    sku: str
    from_market: str
    to_markets: list[str] = Field(default_factory=list)
    drafts_created: int = 0
    skipped: int = 0
    warnings: list[str] = Field(default_factory=list)


class AIContentGenerateRequest(BaseModel):
    sku: str
    marketplace_id: str
    mode: AIMode
    constraints_json: dict[str, Any] = Field(default_factory=dict)
    source_market: Optional[str] = None
    fields: list[str] = Field(default_factory=list)
    model: str = "gpt-5.2"


class AIContentGenerateResponse(BaseModel):
    sku: str
    marketplace_id: str
    mode: AIMode
    model: str
    cache_hit: bool
    policy_flags: list[str] = Field(default_factory=list)
    output: ContentFields
    generated_at: datetime


class ContentOnboardPreflightRequest(BaseModel):
    sku_list: list[str] = Field(default_factory=list)
    main_market: str = "DE"
    target_markets: list[str] = Field(default_factory=list)
    auto_create_tasks: bool = False


class ContentOnboardPreflightItem(BaseModel):
    sku: str
    asin: Optional[str] = None
    ean: Optional[str] = None
    brand: Optional[str] = None
    title: Optional[str] = None
    pim_score: int = 0
    family_coverage_pct: float = 0
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    tasks_created: list[str] = Field(default_factory=list)


class ContentOnboardPreflightResponse(BaseModel):
    main_market: str
    target_markets: list[str] = Field(default_factory=list)
    items: list[ContentOnboardPreflightItem] = Field(default_factory=list)
    generated_at: datetime


class ContentQAVerifyRequest(BaseModel):
    sku: Optional[str] = None
    marketplace_id: str = "DE"
    product_type: Optional[str] = None
    target_language: str = "de_DE"
    pim_facts_json: dict[str, Any] = Field(default_factory=dict)
    content: ContentFields


class ContentQAFinding(BaseModel):
    category: str
    severity: PolicySeverity
    field: str
    message: str
    suggestion: Optional[str] = None


class ContentQAVerifyResponse(BaseModel):
    status: QAVerifyStatus
    score: float
    critical_count: int
    major_count: int
    minor_count: int
    findings: list[ContentQAFinding] = Field(default_factory=list)
    checks_json: dict[str, Any] = Field(default_factory=dict)
    checked_at: datetime


class ContentOnboardRestrictionResponse(BaseModel):
    asin: str
    marketplace: str
    can_list: bool
    requires_approval: bool
    reasons: list[str] = Field(default_factory=list)


class ContentOnboardCatalogMatch(BaseModel):
    asin: str
    title: Optional[str] = None
    brand: Optional[str] = None
    product_type: Optional[str] = None
    image_url: Optional[str] = None


class ContentOnboardCatalogResponse(BaseModel):
    query: str
    marketplace: str
    total: int
    matches: list[ContentOnboardCatalogMatch] = Field(default_factory=list)


class ContentProductTypeMapRule(BaseModel):
    id: Optional[str] = None
    marketplace_id: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    product_type: str
    required_attrs: list[str] = Field(default_factory=list)
    priority: int = 100
    is_active: bool = True


class ContentProductTypeMapUpsertRequest(BaseModel):
    rules: list[ContentProductTypeMapRule] = Field(default_factory=list)


class ContentProductTypeDefinitionItem(BaseModel):
    id: Optional[str] = None
    marketplace_id: str
    marketplace_code: str
    product_type: str
    requirements_json: dict[str, Any] = Field(default_factory=dict)
    required_attrs: list[str] = Field(default_factory=list)
    refreshed_at: datetime
    source: str = "sp_api_definitions"


class ContentProductTypeDefinitionRefreshRequest(BaseModel):
    marketplace: str
    product_type: str
    force_refresh: bool = False


class ContentAttributeMapRule(BaseModel):
    id: Optional[str] = None
    marketplace_id: Optional[str] = None
    product_type: Optional[str] = None
    source_field: str
    target_attribute: str
    transform: Optional[Literal["identity", "stringify", "upper", "lower", "trim"]] = "identity"
    priority: int = 100
    is_active: bool = True


class ContentAttributeMapUpsertRequest(BaseModel):
    rules: list[ContentAttributeMapRule] = Field(default_factory=list)


class ContentPublishCoverageRow(BaseModel):
    marketplace_id: str
    category: Optional[str] = None
    product_type: str
    total_candidates: int
    fully_covered: int
    coverage_pct: float
    missing_required_top: list[str] = Field(default_factory=list)


class ContentPublishCoverageResponse(BaseModel):
    generated_at: datetime
    items: list[ContentPublishCoverageRow] = Field(default_factory=list)


class ContentPublishMappingSuggestionItem(BaseModel):
    marketplace_id: str
    product_type: str
    missing_attribute: str
    suggested_source_field: Optional[str] = None
    confidence: float = 0
    candidates: list[str] = Field(default_factory=list)
    affected_skus: int = 0


class ContentPublishMappingSuggestionsResponse(BaseModel):
    generated_at: datetime
    items: list[ContentPublishMappingSuggestionItem] = Field(default_factory=list)


class ContentPublishQueueHealthResponse(BaseModel):
    generated_at: datetime
    queued_total: int = 0
    queued_stale_30m: int = 0
    running_total: int = 0
    retry_in_progress: int = 0
    failed_last_24h: int = 0
    max_retry_reached_last_24h: int = 0
    thresholds: dict[str, int] = Field(default_factory=dict)


class ContentPublishRetryRequest(BaseModel):
    sku_filter: list[str] = Field(default_factory=list)
    failed_only: bool = True
    idempotency_key: Optional[str] = None


class ContentPublishMappingApplyRequest(BaseModel):
    marketplaces: list[str] = Field(default_factory=list)
    selection: PublishSelection = "approved"
    min_confidence: float = 70
    limit: int = 100
    dry_run: bool = False


class ContentPublishMappingApplyResponse(BaseModel):
    generated_at: datetime
    dry_run: bool
    evaluated: int
    created: int
    skipped: int
    items: list[dict[str, Any]] = Field(default_factory=list)


class ContentOpsHealthResponse(BaseModel):
    generated_at: datetime
    queue_health: ContentPublishQueueHealthResponse
    compliance_backlog: dict[str, int] = Field(default_factory=dict)
    tasks_health: dict[str, int] = Field(default_factory=dict)
    data_quality_cards: list[ContentDataQualityCard] = Field(default_factory=list)


class ContentComplianceQueueItem(BaseModel):
    version_id: str
    sku: str
    marketplace_id: str
    version_no: int
    version_status: VersionStatus
    critical_count: int
    major_count: int
    minor_count: int
    findings: list[dict[str, Any]] = Field(default_factory=list)
    checked_at: datetime


class ContentComplianceQueueResponse(BaseModel):
    total: int
    page: int
    page_size: int
    pages: int
    items: list[ContentComplianceQueueItem] = Field(default_factory=list)


class ContentImpactPoint(BaseModel):
    label: str
    units: int = 0
    revenue: float = 0
    impact_margin_pln: float = 0
    refunds: float = 0
    return_rate: float = 0
    sessions: Optional[int] = None
    cvr: Optional[float] = None


class ContentImpactResponse(BaseModel):
    sku: str
    marketplace_id: str
    range_days: int
    before: ContentImpactPoint
    after: ContentImpactPoint
    delta: ContentImpactPoint
    baseline_expected: ContentImpactPoint
    delta_vs_baseline: ContentImpactPoint
    impact_signal: Literal["negative", "neutral", "positive"]
    confidence_score: float = 0
    baseline_hint: str
    negative_impact: bool
    generated_at: datetime


class ContentDataQualityCard(BaseModel):
    key: str
    value: float
    unit: str = "pct"
    note: Optional[str] = None


class ContentDataQualityResponse(BaseModel):
    cards: list[ContentDataQualityCard] = Field(default_factory=list)
    missing_title: list[dict[str, Any]] = Field(default_factory=list)
    missing_bullets: list[dict[str, Any]] = Field(default_factory=list)
    missing_description: list[dict[str, Any]] = Field(default_factory=list)
    generated_at: datetime
