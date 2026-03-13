"""Content Optimization Engine — scoring, SEO analysis, optimization recommendations.

Sprint 17 (Part 1 of 2-sprint epic):
  - Content quality score model (0-100) per listing per marketplace
  - SEO gap analysis against Brand Analytics search terms
  - Optimization opportunity ranking
  - Score persistence + history tracking

Tables:
  acc_content_score          — Current content quality score per SKU/marketplace
  acc_seo_analysis           — SEO keyword analysis per SKU/marketplace
  acc_content_score_history  — Daily score snapshots for trend tracking

Data sources read:
  acc_listing_state          — Title, brand, image
  acc_co_versions            — Full content: bullets, description, keywords, A+ content
  brand_analytics search_terms — Search term relevance data
"""
from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

import structlog

from app.connectors.mssql import connect_acc

log = structlog.get_logger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

SCORE_VERSION = 1

# Sub-score weights (sum = 100)
WEIGHT_TITLE = 25
WEIGHT_BULLETS = 25
WEIGHT_DESCRIPTION = 15
WEIGHT_KEYWORDS = 15
WEIGHT_IMAGES = 10
WEIGHT_APLUS = 10

# Title scoring thresholds
TITLE_MIN_GOOD = 80
TITLE_MAX_GOOD = 200
TITLE_IDEAL_MIN = 100
TITLE_IDEAL_MAX = 150

# Bullet scoring thresholds
BULLET_IDEAL_COUNT = 5
BULLET_MIN_GOOD_LEN = 50
BULLET_IDEAL_LEN = 120
BULLET_MAX_LEN = 256

# Description thresholds
DESC_MIN_GOOD = 200
DESC_IDEAL_MIN = 500

# Keyword thresholds
KW_MIN_GOOD = 50
KW_IDEAL_MIN = 150
KW_MAX = 250

# Image thresholds
IMAGE_IDEAL_COUNT = 7

# SEO
SEO_MAX_SEARCH_TERMS = 20

# ═══════════════════════════════════════════════════════════════════════════
#  Schema DDL
# ═══════════════════════════════════════════════════════════════════════════

_CONTENT_OPT_SCHEMA: list[str] = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_score')
    CREATE TABLE dbo.acc_content_score (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(60)  NOT NULL,
        asin            NVARCHAR(20)  NULL,
        marketplace_id  NVARCHAR(20)  NOT NULL,
        total_score     INT           NOT NULL DEFAULT 0,
        title_score     INT           NOT NULL DEFAULT 0,
        bullet_score    INT           NOT NULL DEFAULT 0,
        description_score INT         NOT NULL DEFAULT 0,
        keyword_score   INT           NOT NULL DEFAULT 0,
        image_score     INT           NOT NULL DEFAULT 0,
        aplus_score     INT           NOT NULL DEFAULT 0,
        title_length    INT           NULL,
        bullet_count    INT           NULL,
        avg_bullet_len  INT           NULL,
        description_length INT        NULL,
        keyword_length  INT           NULL,
        image_count     INT           NULL,
        has_aplus       BIT           NOT NULL DEFAULT 0,
        issues_json     NVARCHAR(MAX) NULL,
        recommendations_json NVARCHAR(MAX) NULL,
        score_version   INT           NOT NULL DEFAULT 1,
        scored_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_content_score_sku_mkt UNIQUE (seller_sku, marketplace_id)
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_seo_analysis')
    CREATE TABLE dbo.acc_seo_analysis (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(60)  NOT NULL,
        asin            NVARCHAR(20)  NULL,
        marketplace_id  NVARCHAR(20)  NOT NULL,
        seo_score       INT           NOT NULL DEFAULT 0,
        keyword_coverage_pct DECIMAL(5,1) NULL,
        missing_keywords_json NVARCHAR(MAX) NULL,
        top_search_terms_json NVARCHAR(MAX) NULL,
        keyword_density_json  NVARCHAR(MAX) NULL,
        title_keyword_count   INT     NULL,
        title_has_brand       BIT     NOT NULL DEFAULT 0,
        title_has_primary_kw  BIT     NOT NULL DEFAULT 0,
        analyzed_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_seo_sku_mkt UNIQUE (seller_sku, marketplace_id)
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_content_score_history')
    CREATE TABLE dbo.acc_content_score_history (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(60)  NOT NULL,
        marketplace_id  NVARCHAR(20)  NOT NULL,
        total_score     INT           NOT NULL,
        title_score     INT           NOT NULL DEFAULT 0,
        bullet_score    INT           NOT NULL DEFAULT 0,
        description_score INT         NOT NULL DEFAULT 0,
        keyword_score   INT           NOT NULL DEFAULT 0,
        image_score     INT           NOT NULL DEFAULT 0,
        aplus_score     INT           NOT NULL DEFAULT 0,
        snapshot_date   DATE          NOT NULL DEFAULT CAST(SYSUTCDATETIME() AS DATE),
        CONSTRAINT uq_cs_hist_sku_mkt_date UNIQUE (seller_sku, marketplace_id, snapshot_date)
    );
    """,
]


def ensure_content_optimization_schema() -> None:
    """Create tables if needed."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        for ddl in _CONTENT_OPT_SCHEMA:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Pure scoring functions
# ═══════════════════════════════════════════════════════════════════════════

def _fv(val: Any) -> float | None:
    """Safe float conversion."""
    if val is None:
        return None
    try:
        return round(float(val), 2)
    except (TypeError, ValueError):
        return None


def score_title(title: str | None) -> tuple[int, list[str], list[str]]:
    """Score title quality (0-100).

    Returns (score, issues, recommendations).
    """
    issues: list[str] = []
    recs: list[str] = []

    if not title or len(title.strip()) == 0:
        return 0, ["missing_title"], ["Add a product title"]

    length = len(title.strip())

    if length < 30:
        issues.append("title_too_short")
        recs.append(f"Title is only {length} chars — aim for {TITLE_IDEAL_MIN}-{TITLE_IDEAL_MAX}")
        return 15, issues, recs

    score = 100
    # Length scoring
    if length < TITLE_MIN_GOOD:
        score -= 30
        recs.append(f"Title is {length} chars — expand to {TITLE_IDEAL_MIN}+ for better visibility")
    elif length > TITLE_MAX_GOOD:
        score -= 20
        issues.append("title_too_long")
        recs.append(f"Title is {length} chars — trim to under {TITLE_MAX_GOOD}")
    elif TITLE_IDEAL_MIN <= length <= TITLE_IDEAL_MAX:
        pass  # Perfect range
    else:
        score -= 5  # Acceptable but not ideal

    # All caps check
    if title == title.upper() and len(title) > 10:
        score -= 15
        issues.append("title_all_caps")
        recs.append("Avoid ALL CAPS — use Title Case for readability")

    # Special characters check
    if re.search(r'[!@#$%^&*(){}|<>]', title):
        score -= 10
        issues.append("title_special_chars")
        recs.append("Remove special characters from title")

    return max(0, min(100, score)), issues, recs


def score_bullets(
    bullets: list[str] | None,
) -> tuple[int, int, int, list[str], list[str]]:
    """Score bullet points quality (0-100).

    Returns (score, bullet_count, avg_len, issues, recommendations).
    """
    issues: list[str] = []
    recs: list[str] = []

    if not bullets or len(bullets) == 0:
        return 0, 0, 0, ["no_bullets"], ["Add at least 5 bullet points highlighting key features"]

    count = len(bullets)
    lengths = [len(b.strip()) for b in bullets if b.strip()]
    avg_len = sum(lengths) // max(len(lengths), 1)

    score = 100

    # Count scoring
    if count < 3:
        score -= 40
        issues.append("too_few_bullets")
        recs.append(f"Only {count} bullets — add {BULLET_IDEAL_COUNT - count} more")
    elif count < BULLET_IDEAL_COUNT:
        score -= (BULLET_IDEAL_COUNT - count) * 8
        recs.append(f"{count} bullets — ideally use all {BULLET_IDEAL_COUNT}")

    # Length scoring
    short_bullets = [i + 1 for i, l in enumerate(lengths) if l < BULLET_MIN_GOOD_LEN]
    if short_bullets:
        score -= len(short_bullets) * 5
        issues.append("short_bullets")
        recs.append(f"Bullets {short_bullets} are too short — aim for {BULLET_IDEAL_LEN}+ chars")

    long_bullets = [i + 1 for i, l in enumerate(lengths) if l > BULLET_MAX_LEN]
    if long_bullets:
        score -= len(long_bullets) * 3
        issues.append("long_bullets")

    # Duplicates check
    normalized = [b.strip().lower() for b in bullets if b.strip()]
    if len(set(normalized)) < len(normalized):
        score -= 20
        issues.append("duplicate_bullets")
        recs.append("Remove duplicate bullet points")

    # All caps check
    all_caps = sum(1 for b in bullets if b == b.upper() and len(b) > 10)
    if all_caps > 1:
        score -= 10
        issues.append("bullets_all_caps")

    return max(0, min(100, score)), count, avg_len, issues, recs


def score_description(description: str | None) -> tuple[int, int, list[str], list[str]]:
    """Score description quality (0-100).

    Returns (score, length, issues, recommendations).
    """
    issues: list[str] = []
    recs: list[str] = []

    if not description or len(description.strip()) == 0:
        return 0, 0, ["no_description"], ["Add a product description (500+ chars recommended)"]

    length = len(description.strip())

    if length < 50:
        return 10, length, ["description_too_short"], [f"Description is only {length} chars — expand to {DESC_IDEAL_MIN}+"]

    score = 100

    if length < DESC_MIN_GOOD:
        score -= 30
        recs.append(f"Description is {length} chars — expand to {DESC_IDEAL_MIN}+ for better conversion")
    elif length < DESC_IDEAL_MIN:
        score -= 10

    # HTML formatting check (optional bonus)
    if '<' in description and '>' in description:
        score = min(100, score + 5)  # bonus for HTML formatting

    return max(0, min(100, score)), length, issues, recs


def score_keywords(keywords: str | None) -> tuple[int, int, list[str], list[str]]:
    """Score backend keywords quality (0-100).

    Returns (score, length, issues, recommendations).
    """
    issues: list[str] = []
    recs: list[str] = []

    if not keywords or len(keywords.strip()) == 0:
        return 0, 0, ["no_keywords"], ["Add backend search keywords (150-250 chars)"]

    length = len(keywords.strip())

    if length < 20:
        return 10, length, ["keywords_too_short"], [f"Only {length} chars of keywords — use {KW_IDEAL_MIN}-{KW_MAX}"]

    score = 100

    if length < KW_MIN_GOOD:
        score -= 30
        recs.append(f"Keywords are {length} chars — expand to {KW_IDEAL_MIN} for better discoverability")
    elif length < KW_IDEAL_MIN:
        score -= 10
    elif length > KW_MAX:
        score -= 15
        issues.append("keywords_too_long")
        recs.append(f"Keywords exceed {KW_MAX} char limit — Amazon may truncate")

    # Comma-separated (common mistake — Amazon uses spaces)
    if ',' in keywords:
        score -= 10
        issues.append("keywords_commas")
        recs.append("Use spaces instead of commas in backend keywords")

    # Repeated words check
    words = keywords.lower().split()
    unique_words = set(words)
    if len(words) > 5 and len(unique_words) < len(words) * 0.7:
        score -= 15
        issues.append("keywords_duplicated")
        recs.append("Remove duplicate words from keywords to maximize coverage")

    return max(0, min(100, score)), length, issues, recs


def score_images(image_count: int | None, has_aplus: bool = False) -> tuple[int, int, list[str], list[str]]:
    """Score image quality (0-100).

    Returns (score, aplus_score, issues, recommendations).
    """
    issues: list[str] = []
    recs: list[str] = []
    count = image_count or 0

    if count == 0:
        return 0, 0, ["no_images"], ["Add product images — 7+ recommended"]

    score = 100
    if count < 3:
        score -= 40
        issues.append("too_few_images")
        recs.append(f"Only {count} images — add {IMAGE_IDEAL_COUNT - count} more")
    elif count < IMAGE_IDEAL_COUNT:
        score -= (IMAGE_IDEAL_COUNT - count) * 6

    aplus_score = 100 if has_aplus else 0
    if not has_aplus:
        recs.append("Add A+ Content for higher conversion rates")

    return max(0, min(100, score)), aplus_score, issues, recs


def compute_content_score(
    *,
    title: str | None = None,
    bullets: list[str] | None = None,
    description: str | None = None,
    keywords: str | None = None,
    image_count: int | None = None,
    has_aplus: bool = False,
) -> dict[str, Any]:
    """Compute comprehensive content quality score.

    Returns dict with total_score, sub-scores, metrics, issues, recommendations.
    """
    t_score, t_issues, t_recs = score_title(title)
    b_score, b_count, b_avg, b_issues, b_recs = score_bullets(bullets)
    d_score, d_len, d_issues, d_recs = score_description(description)
    k_score, k_len, k_issues, k_recs = score_keywords(keywords)
    i_score, a_score, i_issues, i_recs = score_images(image_count, has_aplus)

    total = round(
        (t_score * WEIGHT_TITLE +
         b_score * WEIGHT_BULLETS +
         d_score * WEIGHT_DESCRIPTION +
         k_score * WEIGHT_KEYWORDS +
         i_score * WEIGHT_IMAGES +
         a_score * WEIGHT_APLUS) / 100
    )
    total = max(0, min(100, total))

    all_issues = t_issues + b_issues + d_issues + k_issues + i_issues
    all_recs = t_recs + b_recs + d_recs + k_recs + i_recs

    return {
        "total_score": total,
        "title_score": t_score,
        "bullet_score": b_score,
        "description_score": d_score,
        "keyword_score": k_score,
        "image_score": i_score,
        "aplus_score": a_score,
        "title_length": len(title.strip()) if title else 0,
        "bullet_count": b_count,
        "avg_bullet_len": b_avg,
        "description_length": d_len,
        "keyword_length": k_len,
        "image_count": image_count or 0,
        "has_aplus": has_aplus,
        "issues": all_issues,
        "recommendations": all_recs,
        "score_version": SCORE_VERSION,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  SEO analysis
# ═══════════════════════════════════════════════════════════════════════════

def analyze_seo(
    *,
    title: str | None = None,
    bullets: list[str] | None = None,
    description: str | None = None,
    keywords: str | None = None,
    brand: str | None = None,
    search_terms: list[dict] | None = None,
) -> dict[str, Any]:
    """Analyze SEO quality by comparing listing content against search terms.

    search_terms: list of {search_term, search_frequency_rank, click_share, conversion_share}

    Returns dict with seo_score, keyword_coverage_pct, missing_keywords, etc.
    """
    # Build combined content text for keyword matching
    content_parts: list[str] = []
    if title:
        content_parts.append(title.lower())
    if bullets:
        content_parts.extend(b.lower() for b in bullets if b)
    if description:
        content_parts.append(description.lower())
    if keywords:
        content_parts.append(keywords.lower())

    full_text = " ".join(content_parts)
    content_words = set(re.findall(r'\b\w{3,}\b', full_text))

    title_lower = (title or "").lower()
    brand_lower = (brand or "").lower()

    # Analyze against search terms
    if not search_terms:
        # Basic analysis without search term data
        title_has_brand = brand_lower in title_lower if brand_lower else False
        return {
            "seo_score": 50,  # baseline when no search data available
            "keyword_coverage_pct": None,
            "missing_keywords": [],
            "top_search_terms": [],
            "keyword_density": {},
            "title_keyword_count": len(re.findall(r'\b\w{3,}\b', title_lower)) if title else 0,
            "title_has_brand": title_has_brand,
            "title_has_primary_kw": False,
        }

    # Only consider top N search terms
    terms = sorted(search_terms, key=lambda t: t.get("search_frequency_rank", 999999))[:SEO_MAX_SEARCH_TERMS]

    covered = 0
    missing: list[dict] = []
    for t in terms:
        st = t.get("search_term", "").lower()
        st_words = set(re.findall(r'\b\w{3,}\b', st))
        # Partial match: at least 50% of search term words in content
        if st_words and len(st_words & content_words) >= max(1, len(st_words) * 0.5):
            covered += 1
        else:
            missing.append({
                "search_term": t.get("search_term", ""),
                "rank": t.get("search_frequency_rank"),
                "click_share": _fv(t.get("click_share")),
                "conversion_share": _fv(t.get("conversion_share")),
            })

    coverage_pct = round(covered / len(terms) * 100, 1) if terms else 0

    # Check title for primary keyword (first search term)
    primary_kw = terms[0].get("search_term", "").lower() if terms else ""
    primary_words = set(re.findall(r'\b\w{3,}\b', primary_kw))
    title_has_primary = bool(primary_words and len(primary_words & set(re.findall(r'\b\w{3,}\b', title_lower))) >= max(1, len(primary_words) * 0.5))

    # Keyword density in title
    title_words = re.findall(r'\b\w{3,}\b', title_lower)
    kw_density: dict[str, int] = {}
    for t in terms[:10]:
        st = t.get("search_term", "").lower()
        for word in re.findall(r'\b\w{3,}\b', st):
            if word in title_lower:
                kw_density[word] = kw_density.get(word, 0) + 1

    # SEO score calculation
    seo_score = 0
    # Coverage component (0-50)
    seo_score += round(coverage_pct * 0.5)
    # Title brand (0-10)
    title_has_brand = brand_lower in title_lower if brand_lower else False
    seo_score += 10 if title_has_brand else 0
    # Title primary kw (0-20)
    seo_score += 20 if title_has_primary else 0
    # Keyword variety (0-20)
    variety = min(20, len(kw_density) * 2)
    seo_score += variety

    seo_score = max(0, min(100, seo_score))

    return {
        "seo_score": seo_score,
        "keyword_coverage_pct": coverage_pct,
        "missing_keywords": missing[:10],
        "top_search_terms": [
            {
                "search_term": t.get("search_term", ""),
                "rank": t.get("search_frequency_rank"),
                "in_content": t.get("search_term", "").lower() in full_text or any(
                    w in content_words for w in re.findall(r'\b\w{3,}\b', t.get("search_term", "").lower())
                ),
            }
            for t in terms[:10]
        ],
        "keyword_density": kw_density,
        "title_keyword_count": len(title_words),
        "title_has_brand": title_has_brand,
        "title_has_primary_kw": title_has_primary,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Pipeline — score listings for a marketplace
# ═══════════════════════════════════════════════════════════════════════════

def score_listings_for_marketplace(
    marketplace_id: str,
    *,
    limit: int = 500,
) -> dict[str, Any]:
    """Score all active listings for a marketplace.

    Reads listing state + latest content versions, computes scores, upserts.
    Returns summary.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # Fetch active listings with content data
        cur.execute("""
            SELECT ls.seller_sku, ls.asin, ls.marketplace_id,
                   ls.title, ls.brand, ls.image_url
            FROM dbo.acc_listing_state ls WITH (NOLOCK)
            WHERE ls.marketplace_id = %s
              AND ls.listing_status = 'ACTIVE'
            ORDER BY ls.seller_sku
            OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY
        """, (marketplace_id, limit))
        listings = cur.fetchall()

        scored = 0
        total_score_sum = 0

        for row in listings:
            sku, asin, mkt, title, brand, image_url = row[0], row[1], row[2], row[3], row[4], row[5]

            # Get latest content version for this SKU/marketplace
            cur.execute("""
                SELECT TOP 1 fields_json
                FROM dbo.acc_co_versions WITH (NOLOCK)
                WHERE sku = %s AND marketplace_id = %s
                  AND status IN ('approved', 'published', 'review', 'draft')
                ORDER BY version_no DESC
            """, (sku, mkt))
            ver_row = cur.fetchone()

            bullets: list[str] = []
            description = None
            kw_text = None
            has_aplus = False
            image_count = 1 if image_url else 0

            if ver_row and ver_row[0]:
                try:
                    fields = json.loads(ver_row[0])
                    bullets = fields.get("bullets", []) or []
                    description = fields.get("description")
                    kw_text = fields.get("keywords")
                    has_aplus = bool(fields.get("aplus_json"))
                    # Count images from fields if available
                    if fields.get("images"):
                        image_count = len(fields["images"])
                except (json.JSONDecodeError, TypeError):
                    pass

            result = compute_content_score(
                title=title,
                bullets=bullets,
                description=description,
                keywords=kw_text,
                image_count=image_count,
                has_aplus=has_aplus,
            )

            # MERGE upsert
            cur.execute("""
                MERGE dbo.acc_content_score AS tgt
                USING (SELECT %s AS seller_sku, %s AS marketplace_id) AS src
                ON tgt.seller_sku = src.seller_sku AND tgt.marketplace_id = src.marketplace_id
                WHEN MATCHED THEN
                    UPDATE SET
                        asin = %s,
                        total_score = %s, title_score = %s, bullet_score = %s,
                        description_score = %s, keyword_score = %s, image_score = %s,
                        aplus_score = %s, title_length = %s, bullet_count = %s,
                        avg_bullet_len = %s, description_length = %s, keyword_length = %s,
                        image_count = %s, has_aplus = %s,
                        issues_json = %s, recommendations_json = %s,
                        score_version = %s, scored_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (seller_sku, asin, marketplace_id,
                            total_score, title_score, bullet_score,
                            description_score, keyword_score, image_score,
                            aplus_score, title_length, bullet_count,
                            avg_bullet_len, description_length, keyword_length,
                            image_count, has_aplus,
                            issues_json, recommendations_json, score_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                # USING
                sku, mkt,
                # UPDATE
                asin,
                result["total_score"], result["title_score"], result["bullet_score"],
                result["description_score"], result["keyword_score"], result["image_score"],
                result["aplus_score"], result["title_length"], result["bullet_count"],
                result["avg_bullet_len"], result["description_length"], result["keyword_length"],
                result["image_count"], 1 if result["has_aplus"] else 0,
                json.dumps(result["issues"]), json.dumps(result["recommendations"]),
                SCORE_VERSION,
                # INSERT
                sku, asin, mkt,
                result["total_score"], result["title_score"], result["bullet_score"],
                result["description_score"], result["keyword_score"], result["image_score"],
                result["aplus_score"], result["title_length"], result["bullet_count"],
                result["avg_bullet_len"], result["description_length"], result["keyword_length"],
                result["image_count"], 1 if result["has_aplus"] else 0,
                json.dumps(result["issues"]), json.dumps(result["recommendations"]),
                SCORE_VERSION,
            ))

            # History snapshot
            cur.execute("""
                MERGE dbo.acc_content_score_history AS tgt
                USING (SELECT %s AS seller_sku, %s AS marketplace_id,
                              CAST(SYSUTCDATETIME() AS DATE) AS snapshot_date) AS src
                ON tgt.seller_sku = src.seller_sku
                   AND tgt.marketplace_id = src.marketplace_id
                   AND tgt.snapshot_date = src.snapshot_date
                WHEN MATCHED THEN
                    UPDATE SET total_score = %s, title_score = %s, bullet_score = %s,
                               description_score = %s, keyword_score = %s,
                               image_score = %s, aplus_score = %s
                WHEN NOT MATCHED THEN
                    INSERT (seller_sku, marketplace_id, total_score, title_score,
                            bullet_score, description_score, keyword_score,
                            image_score, aplus_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                sku, mkt,
                # UPDATE
                result["total_score"], result["title_score"], result["bullet_score"],
                result["description_score"], result["keyword_score"],
                result["image_score"], result["aplus_score"],
                # INSERT
                sku, mkt,
                result["total_score"], result["title_score"], result["bullet_score"],
                result["description_score"], result["keyword_score"],
                result["image_score"], result["aplus_score"],
            ))

            scored += 1
            total_score_sum += result["total_score"]

        conn.commit()
        avg_score = round(total_score_sum / scored, 1) if scored > 0 else 0
        log.info("content_optimization.scored", marketplace_id=marketplace_id,
                 listings=scored, avg_score=avg_score)
        return {
            "marketplace_id": marketplace_id,
            "listings_scored": scored,
            "avg_score": avg_score,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Query functions
# ═══════════════════════════════════════════════════════════════════════════

def get_content_scores(
    marketplace_id: str | None = None,
    *,
    min_score: int | None = None,
    max_score: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Get paginated content scores with optional filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        if min_score is not None:
            where.append("total_score >= %s")
            params.append(min_score)
        if max_score is not None:
            where.append("total_score <= %s")
            params.append(max_score)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_content_score WITH (NOLOCK) WHERE {where_sql}",
            params,
        )
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, seller_sku, asin, marketplace_id,
                   total_score, title_score, bullet_score,
                   description_score, keyword_score, image_score, aplus_score,
                   title_length, bullet_count, avg_bullet_len,
                   description_length, keyword_length, image_count, has_aplus,
                   issues_json, recommendations_json,
                   score_version, scored_at
            FROM dbo.acc_content_score WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY total_score ASC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, params + [offset, limit])

        items = [_score_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_content_score_for_sku(seller_sku: str, marketplace_id: str) -> dict | None:
    """Get content score for a specific SKU."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, seller_sku, asin, marketplace_id,
                   total_score, title_score, bullet_score,
                   description_score, keyword_score, image_score, aplus_score,
                   title_length, bullet_count, avg_bullet_len,
                   description_length, keyword_length, image_count, has_aplus,
                   issues_json, recommendations_json,
                   score_version, scored_at
            FROM dbo.acc_content_score WITH (NOLOCK)
            WHERE seller_sku = %s AND marketplace_id = %s
        """, (seller_sku, marketplace_id))
        row = cur.fetchone()
        return _score_row_to_dict(row) if row else None
    finally:
        conn.close()


def get_score_distribution(marketplace_id: str | None = None) -> dict[str, Any]:
    """Get score distribution for dashboard (buckets: 0-20, 21-40, 41-60, 61-80, 81-100)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = []
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT
                COUNT(CASE WHEN total_score BETWEEN 0 AND 20 THEN 1 END) AS poor,
                COUNT(CASE WHEN total_score BETWEEN 21 AND 40 THEN 1 END) AS below_avg,
                COUNT(CASE WHEN total_score BETWEEN 41 AND 60 THEN 1 END) AS average,
                COUNT(CASE WHEN total_score BETWEEN 61 AND 80 THEN 1 END) AS good,
                COUNT(CASE WHEN total_score BETWEEN 81 AND 100 THEN 1 END) AS excellent,
                COUNT(*) AS total,
                AVG(CAST(total_score AS FLOAT)) AS avg_score,
                AVG(CAST(title_score AS FLOAT)) AS avg_title,
                AVG(CAST(bullet_score AS FLOAT)) AS avg_bullet,
                AVG(CAST(description_score AS FLOAT)) AS avg_description,
                AVG(CAST(keyword_score AS FLOAT)) AS avg_keyword,
                AVG(CAST(image_score AS FLOAT)) AS avg_image,
                AVG(CAST(aplus_score AS FLOAT)) AS avg_aplus
            FROM dbo.acc_content_score WITH (NOLOCK)
            WHERE 1=1 {mkt_filter}
        """, params)
        row = cur.fetchone()

        if not row or (row[5] or 0) == 0:
            return {
                "distribution": {"poor": 0, "below_avg": 0, "average": 0, "good": 0, "excellent": 0},
                "total": 0, "avg_score": None, "avg_by_component": {},
            }

        return {
            "distribution": {
                "poor": row[0] or 0,
                "below_avg": row[1] or 0,
                "average": row[2] or 0,
                "good": row[3] or 0,
                "excellent": row[4] or 0,
            },
            "total": row[5] or 0,
            "avg_score": _fv(row[6]),
            "avg_by_component": {
                "title": _fv(row[7]),
                "bullets": _fv(row[8]),
                "description": _fv(row[9]),
                "keywords": _fv(row[10]),
                "images": _fv(row[11]),
                "aplus": _fv(row[12]),
            },
        }
    finally:
        conn.close()


def get_top_opportunities(
    marketplace_id: str | None = None,
    *,
    limit: int = 20,
) -> list[dict]:
    """Get lowest-scoring listings (biggest improvement opportunities)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [limit]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT TOP (%s)
                   seller_sku, asin, marketplace_id,
                   total_score, title_score, bullet_score,
                   description_score, keyword_score, image_score, aplus_score,
                   issues_json, recommendations_json
            FROM dbo.acc_content_score WITH (NOLOCK)
            WHERE total_score < 70 {mkt_filter}
            ORDER BY total_score ASC
        """, params)

        return [
            {
                "seller_sku": r[0],
                "asin": r[1],
                "marketplace_id": r[2],
                "total_score": r[3],
                "title_score": r[4],
                "bullet_score": r[5],
                "description_score": r[6],
                "keyword_score": r[7],
                "image_score": r[8],
                "aplus_score": r[9],
                "issues": _safe_json(r[10]),
                "recommendations": _safe_json(r[11]),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def get_score_history(
    seller_sku: str,
    marketplace_id: str,
    *,
    days: int = 30,
) -> list[dict]:
    """Get score history trend for a listing."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT snapshot_date, total_score, title_score, bullet_score,
                   description_score, keyword_score, image_score, aplus_score
            FROM dbo.acc_content_score_history WITH (NOLOCK)
            WHERE seller_sku = %s AND marketplace_id = %s
              AND snapshot_date >= DATEADD(day, -%s, GETUTCDATE())
            ORDER BY snapshot_date ASC
        """, (seller_sku, marketplace_id, days))
        return [
            {
                "date": str(r[0]),
                "total_score": r[1],
                "title_score": r[2],
                "bullet_score": r[3],
                "description_score": r[4],
                "keyword_score": r[5],
                "image_score": r[6],
                "aplus_score": r[7],
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def get_seo_analysis_for_sku(seller_sku: str, marketplace_id: str) -> dict | None:
    """Get stored SEO analysis for a SKU."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, seller_sku, asin, marketplace_id,
                   seo_score, keyword_coverage_pct,
                   missing_keywords_json, top_search_terms_json,
                   keyword_density_json,
                   title_keyword_count, title_has_brand, title_has_primary_kw,
                   analyzed_at
            FROM dbo.acc_seo_analysis WITH (NOLOCK)
            WHERE seller_sku = %s AND marketplace_id = %s
        """, (seller_sku, marketplace_id))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "seller_sku": row[1],
            "asin": row[2],
            "marketplace_id": row[3],
            "seo_score": row[4],
            "keyword_coverage_pct": _fv(row[5]),
            "missing_keywords": _safe_json(row[6]),
            "top_search_terms": _safe_json(row[7]),
            "keyword_density": _safe_json(row[8]),
            "title_keyword_count": row[9],
            "title_has_brand": bool(row[10]),
            "title_has_primary_kw": bool(row[11]),
            "analyzed_at": str(row[12]),
        }
    finally:
        conn.close()


def save_seo_analysis(
    seller_sku: str,
    asin: str | None,
    marketplace_id: str,
    analysis: dict[str, Any],
) -> None:
    """Persist SEO analysis result."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.acc_seo_analysis AS tgt
            USING (SELECT %s AS seller_sku, %s AS marketplace_id) AS src
            ON tgt.seller_sku = src.seller_sku AND tgt.marketplace_id = src.marketplace_id
            WHEN MATCHED THEN
                UPDATE SET
                    asin = %s,
                    seo_score = %s,
                    keyword_coverage_pct = %s,
                    missing_keywords_json = %s,
                    top_search_terms_json = %s,
                    keyword_density_json = %s,
                    title_keyword_count = %s,
                    title_has_brand = %s,
                    title_has_primary_kw = %s,
                    analyzed_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (seller_sku, asin, marketplace_id,
                        seo_score, keyword_coverage_pct,
                        missing_keywords_json, top_search_terms_json,
                        keyword_density_json, title_keyword_count,
                        title_has_brand, title_has_primary_kw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            # USING
            seller_sku, marketplace_id,
            # UPDATE
            asin,
            analysis.get("seo_score", 0),
            analysis.get("keyword_coverage_pct"),
            json.dumps(analysis.get("missing_keywords", [])),
            json.dumps(analysis.get("top_search_terms", [])),
            json.dumps(analysis.get("keyword_density", {})),
            analysis.get("title_keyword_count", 0),
            1 if analysis.get("title_has_brand") else 0,
            1 if analysis.get("title_has_primary_kw") else 0,
            # INSERT
            seller_sku, asin, marketplace_id,
            analysis.get("seo_score", 0),
            analysis.get("keyword_coverage_pct"),
            json.dumps(analysis.get("missing_keywords", [])),
            json.dumps(analysis.get("top_search_terms", [])),
            json.dumps(analysis.get("keyword_density", {})),
            analysis.get("title_keyword_count", 0),
            1 if analysis.get("title_has_brand") else 0,
            1 if analysis.get("title_has_primary_kw") else 0,
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _safe_json(val: Any) -> Any:
    """Parse JSON string or return empty list/dict."""
    if val is None:
        return []
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


def _score_row_to_dict(row) -> dict:
    """Convert SELECT row to dict."""
    return {
        "id": row[0],
        "seller_sku": row[1],
        "asin": row[2],
        "marketplace_id": row[3],
        "total_score": row[4],
        "title_score": row[5],
        "bullet_score": row[6],
        "description_score": row[7],
        "keyword_score": row[8],
        "image_score": row[9],
        "aplus_score": row[10],
        "title_length": row[11],
        "bullet_count": row[12],
        "avg_bullet_len": row[13],
        "description_length": row[14],
        "keyword_length": row[15],
        "image_count": row[16],
        "has_aplus": bool(row[17]),
        "issues": _safe_json(row[18]),
        "recommendations": _safe_json(row[19]),
        "score_version": row[20],
        "scored_at": str(row[21]),
    }
