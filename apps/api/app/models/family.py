"""Family Mapper ORM models — DE Canonical → EU variation family mapping."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


# ---------------------------------------------------------------------------
# 1) global_family
# ---------------------------------------------------------------------------
class GlobalFamily(Base):
    __tablename__ = "global_family"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    de_parent_asin: Mapped[str] = mapped_column(String(20), nullable=False, unique=True, index=True)
    brand: Mapped[str | None] = mapped_column(String(120), nullable=True)
    category: Mapped[str | None] = mapped_column(String(200), nullable=True)
    product_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    variation_theme_de: Mapped[str | None] = mapped_column(String(120), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    children: Mapped[list["GlobalFamilyChild"]] = relationship(
        "GlobalFamilyChild", back_populates="family", cascade="all, delete-orphan"
    )
    market_links: Mapped[list["GlobalFamilyMarketLink"]] = relationship(
        "GlobalFamilyMarketLink", back_populates="family", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<GlobalFamily {self.de_parent_asin}>"


# ---------------------------------------------------------------------------
# 2) global_family_child
# ---------------------------------------------------------------------------
class GlobalFamilyChild(Base):
    __tablename__ = "global_family_child"
    __table_args__ = (
        UniqueConstraint("global_family_id", "master_key", name="UX_gfc_family_master"),
        Index("IX_gfc_master_key", "master_key"),
        Index("IX_gfc_de_child_asin", "de_child_asin"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    global_family_id: Mapped[int] = mapped_column(Integer, ForeignKey("global_family.id"), nullable=False)
    master_key: Mapped[str] = mapped_column(String(120), nullable=False)
    key_type: Mapped[str] = mapped_column(String(20), nullable=False)
    de_child_asin: Mapped[str] = mapped_column(String(20), nullable=False)
    sku_de: Mapped[str | None] = mapped_column(String(80), nullable=True)
    ean_de: Mapped[str | None] = mapped_column(String(20), nullable=True)
    attributes_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    family: Mapped["GlobalFamily"] = relationship("GlobalFamily", back_populates="children")
    child_market_links: Mapped[list["GlobalFamilyChildMarketLink"]] = relationship(
        "GlobalFamilyChildMarketLink",
        back_populates="child",
        foreign_keys="[GlobalFamilyChildMarketLink.global_family_id, GlobalFamilyChildMarketLink.master_key]",
        primaryjoin=(
            "and_(GlobalFamilyChild.global_family_id == GlobalFamilyChildMarketLink.global_family_id, "
            "GlobalFamilyChild.master_key == GlobalFamilyChildMarketLink.master_key)"
        ),
        viewonly=True,
    )

    def __repr__(self) -> str:
        return f"<GlobalFamilyChild {self.de_child_asin} key={self.master_key}>"


# ---------------------------------------------------------------------------
# 3) marketplace_listing_child
# ---------------------------------------------------------------------------
class MarketplaceListingChild(Base):
    __tablename__ = "marketplace_listing_child"
    __table_args__ = (
        Index("IX_mlc_mp_sku", "marketplace", "sku"),
        Index("IX_mlc_mp_ean", "marketplace", "ean"),
        Index("IX_mlc_mp_parent", "marketplace", "current_parent_asin"),
    )

    marketplace: Mapped[str] = mapped_column(String(10), primary_key=True)
    asin: Mapped[str] = mapped_column(String(20), primary_key=True)
    sku: Mapped[str | None] = mapped_column(String(80), nullable=True)
    ean: Mapped[str | None] = mapped_column(String(20), nullable=True)
    current_parent_asin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    variation_theme: Mapped[str | None] = mapped_column(String(120), nullable=True)
    attributes_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<MLC {self.marketplace}:{self.asin}>"


# ---------------------------------------------------------------------------
# 4) global_family_child_market_link
# ---------------------------------------------------------------------------
class GlobalFamilyChildMarketLink(Base):
    __tablename__ = "global_family_child_market_link"
    __table_args__ = (
        Index("IX_gfcl_mp_target_child", "marketplace", "target_child_asin"),
        Index("IX_gfcl_mp_current_parent", "marketplace", "current_parent_asin"),
    )

    global_family_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    master_key: Mapped[str] = mapped_column(String(120), primary_key=True)
    marketplace: Mapped[str] = mapped_column(String(10), primary_key=True)
    target_child_asin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    current_parent_asin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    match_type: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="proposed")
    reason_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    child: Mapped["GlobalFamilyChild | None"] = relationship(
        "GlobalFamilyChild",
        viewonly=True,
        foreign_keys="[GlobalFamilyChildMarketLink.global_family_id, GlobalFamilyChildMarketLink.master_key]",
        primaryjoin=(
            "and_(GlobalFamilyChildMarketLink.global_family_id == GlobalFamilyChild.global_family_id, "
            "GlobalFamilyChildMarketLink.master_key == GlobalFamilyChild.master_key)"
        ),
    )

    def __repr__(self) -> str:
        return f"<GFCML {self.marketplace} key={self.master_key} → {self.target_child_asin}>"


# ---------------------------------------------------------------------------
# 5) global_family_market_link
# ---------------------------------------------------------------------------
class GlobalFamilyMarketLink(Base):
    __tablename__ = "global_family_market_link"

    global_family_id: Mapped[int] = mapped_column(Integer, ForeignKey("global_family.id"), primary_key=True)
    marketplace: Mapped[str] = mapped_column(String(10), primary_key=True)
    target_parent_asin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="unmapped")
    confidence_avg: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    family: Mapped["GlobalFamily"] = relationship("GlobalFamily", back_populates="market_links")

    def __repr__(self) -> str:
        return f"<GFML {self.marketplace} family={self.global_family_id}>"


# ---------------------------------------------------------------------------
# 6) family_coverage_cache
# ---------------------------------------------------------------------------
class FamilyCoverageCache(Base):
    __tablename__ = "family_coverage_cache"

    global_family_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    marketplace: Mapped[str] = mapped_column(String(10), primary_key=True)
    de_children_count: Mapped[int] = mapped_column(Integer, nullable=False)
    matched_children_count: Mapped[int] = mapped_column(Integer, nullable=False)
    coverage_pct: Mapped[int] = mapped_column(Integer, nullable=False)
    missing_children_count: Mapped[int] = mapped_column(Integer, nullable=False)
    extra_children_count: Mapped[int] = mapped_column(Integer, nullable=False)
    theme_mismatch: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    confidence_avg: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<Coverage {self.marketplace} family={self.global_family_id} {self.coverage_pct}%>"


# ---------------------------------------------------------------------------
# 7) family_issues_cache
# ---------------------------------------------------------------------------
class FamilyIssuesCache(Base):
    __tablename__ = "family_issues_cache"
    __table_args__ = (
        Index("IX_fic_family", "global_family_id", "marketplace"),
        Index("IX_fic_severity", "severity", "issue_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    global_family_id: Mapped[int] = mapped_column(Integer, nullable=False)
    marketplace: Mapped[str | None] = mapped_column(String(10), nullable=True)
    issue_type: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return f"<Issue {self.severity} {self.issue_type} family={self.global_family_id}>"


# ---------------------------------------------------------------------------
# 8) family_fix_package
# ---------------------------------------------------------------------------
class FamilyFixPackage(Base):
    __tablename__ = "family_fix_package"
    __table_args__ = (
        Index("IX_ffp_mp_status", "marketplace", "status"),
        Index("IX_ffp_family", "global_family_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    marketplace: Mapped[str] = mapped_column(String(10), nullable=False)
    global_family_id: Mapped[int] = mapped_column(Integer, nullable=False)
    action_plan_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="draft")
    generated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    approved_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<FixPkg {self.marketplace} family={self.global_family_id} {self.status}>"


# ---------------------------------------------------------------------------
# 9) family_fix_job
# ---------------------------------------------------------------------------
class FamilyFixJob(Base):
    __tablename__ = "family_fix_job"
    __table_args__ = (
        Index("IX_ffj_status", "status", "marketplace"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_type: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    marketplace: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    log: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<FamilyFixJob {self.id} {self.job_type} {self.status}>"
