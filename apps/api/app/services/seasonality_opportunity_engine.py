"""Seasonality Opportunity Engine — detect actionable seasonal opportunities.

Generates opportunities from seasonality profiles, execution gaps, and marketplace analysis.
Runs as a scheduler job.
"""
from __future__ import annotations

import json
import math
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY, RENEWED_SKU_SQL_FILTER
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

MKT_CODE = {mid: info["code"] for mid, info in MARKETPLACE_REGISTRY.items()}


def _f(v) -> float:
    if v is None:
        return 0.0
    return float(v)


def _rows(cur) -> list:
    rows = cur.fetchall()
    out = []
    for row in rows:
        conv = []
        for v in row:
            if isinstance(v, Decimal):
                conv.append(float(v))
            else:
                conv.append(v)
        out.append(tuple(conv))
    return out


def detect_seasonality_opportunities() -> dict:
    """Main detection pipeline. Creates new opportunities, skips duplicates."""
    conn = connect_acc()
    cur = conn.cursor()

    today = date.today()
    current_month = today.month
    next_2_months = [(current_month + i - 1) % 12 + 1 for i in range(1, 3)]

    total = 0
    total += _detect_prepare_stock(cur, next_2_months, today)
    total += _detect_prepare_content(cur, next_2_months, today)
    total += _detect_prepare_ads(cur, next_2_months, today)
    total += _detect_prepare_pricing(cur, next_2_months, today)
    total += _detect_execution_gap(cur, today)
    total += _detect_profit_protection(cur, today)
    total += _detect_post_season_liquidation(cur, current_month, today)
    total += _detect_market_expansion(cur, today)

    conn.commit()
    conn.close()
    log.info("seasonality.opportunities.done", total_created=total)
    return {"opportunities_created": total}


def _opp_exists(cur, mkt: str, entity_type: str, entity_id: str,
                opp_type: str) -> bool:
    """Check if an active opportunity already exists."""
    cur.execute("""
        SELECT COUNT(*) FROM seasonality_opportunity
        WHERE marketplace=? AND entity_type=? AND entity_id=?
              AND opportunity_type=? AND status IN ('new','accepted')
    """, (mkt, entity_type, entity_id, opp_type))
    return int(_rows(cur)[0][0]) > 0


def _insert_opp(cur, *, marketplace: str, entity_type: str, entity_id: str,
                opp_type: str, title: str, description: str,
                priority: float, confidence: float,
                revenue_uplift: float | None = None,
                profit_uplift: float | None = None,
                start_date: date | None = None,
                signals: dict | None = None):
    cur.execute("""
        INSERT INTO seasonality_opportunity
            (marketplace, entity_type, entity_id, opportunity_type,
             title, description, priority_score, confidence_score,
             estimated_revenue_uplift, estimated_profit_uplift,
             recommended_start_date, source_signals_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (marketplace, entity_type, entity_id, opp_type,
          title, description, priority, confidence,
          revenue_uplift, profit_uplift, start_date,
          json.dumps(signals) if signals else None))


# ── Detection engines ────────────────────────────────────────────────

def _detect_prepare_stock(cur, next_months: list[int], today: date) -> int:
    """Upcoming peak → check if stock prep needed."""
    cur.execute("""
        SELECT p.marketplace, p.entity_type, p.entity_id,
               p.demand_strength_score, p.seasonality_confidence_score,
               p.peak_months_json
        FROM seasonality_profile p
        WHERE p.seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
              AND p.seasonality_confidence_score >= 40
              AND p.entity_id NOT LIKE 'amzn.gr.%%' AND p.entity_id NOT LIKE 'amazon.found%%'
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        strength, conf = r[3], r[4]
        peaks = json.loads(r[5]) if r[5] else []

        if not any(m in next_months for m in peaks):
            continue
        if _opp_exists(cur, mkt, etype, eid, "PREPARE_STOCK"):
            continue

        pri = round(min(strength * 0.6 + conf * 0.4, 100), 2)
        rev_est = round(strength * 20, 2)

        months_pl = ", ".join(str(m) for m in peaks)
        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type="PREPARE_STOCK",
            title=f"Uzupełnij stany — szczyt w mies. {months_pl} ({mkt})",
            description=(f"Sezonowy szczyt popytu w miesiącach {peaks}. "
                         f"Siła popytu: {strength:.0f}/100. "
                         f"Zapewnij odpowiednie stany magazynowe 6-8 tyg. przed szczytem."),
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.15, 2) if rev_est else None,
            start_date=today,
            signals={"peak_months": peaks, "strength": strength})
        n += 1
        if n >= 200:
            break
    return n


def _detect_prepare_content(cur, next_months: list[int], today: date) -> int:
    """Content refresh before seasonal peak."""
    cur.execute("""
        SELECT p.marketplace, p.entity_type, p.entity_id,
               p.demand_strength_score, p.seasonality_confidence_score,
               p.peak_months_json
        FROM seasonality_profile p
        WHERE p.seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
              AND p.demand_strength_score >= 50
              AND p.entity_id NOT LIKE 'amzn.gr.%%' AND p.entity_id NOT LIKE 'amazon.found%%'
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        strength, conf = r[3], r[4]
        peaks = json.loads(r[5]) if r[5] else []

        if not any(m in next_months for m in peaks):
            continue
        if _opp_exists(cur, mkt, etype, eid, "PREPARE_CONTENT"):
            continue

        pri = round(min(strength * 0.5 + conf * 0.3, 100), 2)

        months_pl = ", ".join(str(m) for m in peaks)
        rev_est = round(strength * 12, 2)
        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type="PREPARE_CONTENT",
            title=f"Odśwież content — szczyt w mies. {months_pl} ({mkt})",
            description=(f"Szczyt w miesiącach {peaks}. Zoptymalizuj listing, zdjęcia "
                         f"i A+ przed wzrostem popytu. Siła: {strength:.0f}/100."),
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.12, 2),
            start_date=today,
            signals={"peak_months": peaks, "strength": strength})
        n += 1
        if n >= 150:
            break
    return n


def _detect_prepare_ads(cur, next_months: list[int], today: date) -> int:
    """Scale ads before seasonal peak."""
    cur.execute("""
        SELECT p.marketplace, p.entity_type, p.entity_id,
               p.demand_strength_score, p.seasonality_confidence_score,
               p.peak_months_json
        FROM seasonality_profile p
        WHERE p.seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
              AND p.demand_strength_score >= 55
              AND p.entity_id NOT LIKE 'amzn.gr.%%' AND p.entity_id NOT LIKE 'amazon.found%%'
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        strength, conf = r[3], r[4]
        peaks = json.loads(r[5]) if r[5] else []

        if not any(m in next_months for m in peaks):
            continue
        if _opp_exists(cur, mkt, etype, eid, "PREPARE_ADS"):
            continue

        pri = round(min(strength * 0.55 + conf * 0.35, 100), 2)

        months_pl = ", ".join(str(m) for m in peaks)
        rev_est = round(strength * 15, 2)
        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type="PREPARE_ADS",
            title=f"Skaluj reklamy — szczyt w mies. {months_pl} ({mkt})",
            description=(f"Szczyt popytu w miesiącach {peaks}. "
                         f"Zwiększ budżet/stawki 4 tyg. przed szczytem. "
                         f"Siła: {strength:.0f}/100."),
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.10, 2),
            start_date=today,
            signals={"peak_months": peaks, "strength": strength})
        n += 1
        if n >= 150:
            break
    return n


def _detect_prepare_pricing(cur, next_months: list[int], today: date) -> int:
    """Seasonal pricing adjustments."""
    cur.execute("""
        SELECT p.marketplace, p.entity_type, p.entity_id,
               p.demand_strength_score, p.profit_strength_score,
               p.seasonality_confidence_score, p.peak_months_json
        FROM seasonality_profile p
        WHERE p.seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
              AND p.profit_strength_score >= 40
              AND p.entity_id NOT LIKE 'amzn.gr.%%' AND p.entity_id NOT LIKE 'amazon.found%%'
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        d_str, p_str, conf = r[3], r[4], r[5]
        peaks = json.loads(r[6]) if r[6] else []

        if not any(m in next_months for m in peaks):
            continue
        if _opp_exists(cur, mkt, etype, eid, "PREPARE_PRICING"):
            continue

        pri = round(min(d_str * 0.4 + p_str * 0.3 + conf * 0.3, 100), 2)

        months_pl = ", ".join(str(m) for m in peaks)
        rev_est = round((d_str + p_str) * 8, 2)
        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type="PREPARE_PRICING",
            title=f"Dostosuj ceny — szczyt w mies. {months_pl} ({mkt})",
            description=(f"Wysoki popyt (siła {d_str:.0f}) przy zyskowności "
                         f"sezonowej ({p_str:.0f}). Zoptymalizuj ceny "
                         f"na miesiące {peaks}."),
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.20, 2),
            start_date=today,
            signals={"peak_months": peaks, "demand_strength": d_str,
                     "profit_strength": p_str})
        n += 1
        if n >= 100:
            break
    return n


def _detect_execution_gap(cur, today: date) -> int:
    """Demand rising but sales/profit not following."""
    cur.execute("""
        SELECT marketplace, entity_type, entity_id,
               demand_vs_sales_gap, sales_vs_profit_gap,
               demand_strength_score, seasonality_confidence_score
        FROM seasonality_profile
        WHERE demand_vs_sales_gap > 0.3 AND seasonality_confidence_score >= 40
              AND entity_id NOT LIKE 'amzn.gr.%%' AND entity_id NOT LIKE 'amazon.found%%'
        ORDER BY demand_vs_sales_gap DESC
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        d_gap, p_gap, strength, conf = r[3], r[4], r[5], r[6]

        opp_type = "PREPARE_CONTENT"  # execution gap → content/strategy fix
        if p_gap and p_gap > 0.3:
            opp_type = "PROFIT_PROTECTION"

        if _opp_exists(cur, mkt, etype, eid, opp_type):
            continue

        pri = round(min(d_gap * 60 + strength * 0.3, 100), 2)
        rev_est = round(d_gap * strength * 10, 2)
        desc = (f"Luka popyt→sprzedaż: {d_gap:.2f}. Luka sprzedaż→zysk: {(p_gap or 0):.2f}. "
                f"Popyt jest silny, ale realizacja (sprzedaż/zysk) nie nadąża. "
                f"Sprawdź content, ceny, stany i reklamy.")

        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type=opp_type,
            title=f"Luka realizacji — popyt nie konwertuje ({mkt})",
            description=desc,
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.15, 2),
            start_date=today,
            signals={"demand_vs_sales_gap": d_gap,
                     "sales_vs_profit_gap": p_gap})
        n += 1
        if n >= 100:
            break
    return n


def _detect_profit_protection(cur, today: date) -> int:
    """Sales peak doesn't convert into profit peak."""
    cur.execute("""
        SELECT marketplace, entity_type, entity_id,
               sales_strength_score, profit_strength_score,
               sales_vs_profit_gap, seasonality_confidence_score
        FROM seasonality_profile
        WHERE sales_vs_profit_gap > 0.3
              AND sales_strength_score >= 40
              AND seasonality_confidence_score >= 40
              AND entity_id NOT LIKE 'amzn.gr.%%' AND entity_id NOT LIKE 'amazon.found%%'
        ORDER BY sales_vs_profit_gap DESC
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        s_str, p_str, gap, conf = r[3], r[4], r[5], r[6]

        if _opp_exists(cur, mkt, etype, eid, "PROFIT_PROTECTION"):
            continue

        pri = round(min(gap * 50 + s_str * 0.3, 100), 2)

        rev_est = round(gap * s_str * 8, 2)
        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type="PROFIT_PROTECTION",
            title=f"Ochrona marży — sprzedaż ≠ zysk ({mkt})",
            description=(f"Siła sprzedaży {s_str:.0f} vs siła zysku {p_str:.0f}. "
                         f"Różnica: {gap:.2f}. Przychody rosną w sezonie, ale koszty zjadają marżę. "
                         f"Sprawdź wydatki na reklamy, koszty FBA, zwroty."),
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.25, 2),
            start_date=today,
            signals={"sales_strength": s_str, "profit_strength": p_str,
                     "gap": gap})
        n += 1
        if n >= 100:
            break
    return n


def _detect_post_season_liquidation(cur, current_month: int, today: date) -> int:
    """Detect products past their peak that may have excess stock."""
    cur.execute("""
        SELECT marketplace, entity_type, entity_id,
               peak_months_json, demand_strength_score,
               seasonality_confidence_score
        FROM seasonality_profile
        WHERE seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
              AND demand_strength_score >= 50
              AND entity_id NOT LIKE 'amzn.gr.%%' AND entity_id NOT LIKE 'amazon.found%%'
    """)
    n = 0
    for r in _rows(cur):
        mkt, etype, eid = r[0], r[1], r[2]
        peaks = json.loads(r[3]) if r[3] else []
        strength, conf = r[4], r[5]

        # Check if peak just passed (1-2 months ago)
        recent_peaks = [p for p in peaks if (current_month - p) % 12 in (1, 2)]
        if not recent_peaks:
            continue
        if _opp_exists(cur, mkt, etype, eid, "LIQUIDATE_POST_SEASON"):
            continue

        pri = round(min(strength * 0.5 + conf * 0.3, 100), 2)

        months_pl = ", ".join(str(m) for m in peaks)
        rev_est = round(strength * 5, 2)
        _insert_opp(
            cur, marketplace=mkt, entity_type=etype, entity_id=eid,
            opp_type="LIQUIDATE_POST_SEASON",
            title=f"Wyprzedaż po sezonie — szczyt {months_pl} minął ({mkt})",
            description=(f"Szczyty {peaks} właśnie minęły. "
                         f"Rozważ ceny likwidacyjne jeśli zostały nadmiarowe stany. "
                         f"Zredukuj reklamy i przenieś budżet na rosnące kategorie."),
            priority=pri, confidence=conf,
            revenue_uplift=rev_est,
            profit_uplift=round(rev_est * 0.05, 2),
            start_date=today,
            signals={"peak_months": peaks, "just_passed": recent_peaks})
        n += 1
        if n >= 100:
            break
    return n


def _detect_market_expansion(cur, today: date) -> int:
    """Strong seasonal pattern in one marketplace -> opportunity in others."""
    cur.execute("""
        SELECT entity_type, entity_id, marketplace,
               demand_strength_score, seasonality_confidence_score,
               peak_months_json
        FROM seasonality_profile
        WHERE seasonality_class IN ('STRONG_SEASONAL','PEAK_SEASONAL')
              AND demand_strength_score >= 60
              AND entity_type = 'sku'
              AND entity_id NOT LIKE 'amzn.gr.%%' AND entity_id NOT LIKE 'amazon.found%%'
        ORDER BY demand_strength_score DESC
    """)
    rows = _rows(cur)

    # Group by entity
    entity_map: dict[tuple, list] = {}
    for r in rows:
        key = (r[0], r[1])
        entity_map.setdefault(key, []).append({
            "marketplace": r[2], "strength": r[3],
            "conf": r[4], "peaks": json.loads(r[5]) if r[5] else []
        })

    n = 0
    all_mkts = set(MKT_CODE.values())
    for (etype, eid), mkt_data in entity_map.items():
        present_mkts = {m["marketplace"] for m in mkt_data}
        missing_mkts = all_mkts - present_mkts

        if not missing_mkts or len(present_mkts) < 2:
            continue

        best = max(mkt_data, key=lambda x: x["strength"])
        for missing_mkt in missing_mkts:
            if _opp_exists(cur, missing_mkt, etype, eid, "MARKET_EXPANSION_PREP"):
                continue

            pri = round(min(best["strength"] * 0.5, 100), 2)

            rev_est = round(best["strength"] * 10, 2)
            _insert_opp(
                cur, marketplace=missing_mkt, entity_type=etype, entity_id=eid,
                opp_type="MARKET_EXPANSION_PREP",
                title=f"Ekspansja na {missing_mkt} — potencjał sezonowy",
                description=(f"Silna sezonowość na {best['marketplace']} "
                             f"(siła {best['strength']:.0f}). "
                             f"Brak obecności na {missing_mkt}. "
                             f"Szczyty: {best['peaks']}."),
                priority=pri, confidence=best["conf"] * 0.7,
                revenue_uplift=rev_est,
                profit_uplift=round(rev_est * 0.08, 2),
                start_date=today,
                signals={"source_marketplace": best["marketplace"],
                         "peak_months": best["peaks"]})
            n += 1
            if n >= 100:
                return n
    return n
