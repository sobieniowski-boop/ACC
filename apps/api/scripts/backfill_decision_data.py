"""Backfill Decision Intelligence data from existing opportunities.

Creates synthetic executions + outcomes from accepted/completed opportunities
to bootstrap the learning system with historical data.
"""
import sys, os, json, random
from datetime import date, timedelta
from decimal import Decimal
sys.path.insert(0, r"C:\ACC\apps\api")
os.environ.setdefault("DATABASE_URL", "mssql+pymssql://acc-sql-kadax.database.windows.net/ACC")
from app.core.db_connection import connect_acc

MONITORING_WINDOWS = {
    "PRICE_INCREASE": [14, 30], "PRICE_DECREASE": [14, 30],
    "ADS_SCALE_UP": [14, 30], "ADS_CUT_WASTE": [14, 30],
    "CONTENT_FIX": [30], "STOCK_REPLENISH": [14, 30],
    "MARKETPLACE_EXPANSION": [30, 60], "FAMILY_REPAIR": [30],
    "RETURN_REDUCTION": [14, 30], "COST_RENEGOTIATION": [30],
    "CATEGORY_WINNER_SCALE": [14, 30], "BUNDLE_CREATE": [30],
    "VARIANT_EXPANSION": [30],
}

ACTION_MAP = {
    "PRICE_INCREASE": "price_change", "PRICE_DECREASE": "price_change",
    "ADS_SCALE_UP": "ads_adjustment", "ADS_CUT_WASTE": "ads_adjustment",
    "CONTENT_FIX": "content_update", "STOCK_REPLENISH": "inventory_fix",
    "MARKETPLACE_EXPANSION": "marketplace_launch", "FAMILY_REPAIR": "family_repair",
    "RETURN_REDUCTION": "return_fix", "COST_RENEGOTIATION": "cost_renegotiation",
    "CATEGORY_WINNER_SCALE": "scale_up", "BUNDLE_CREATE": "bundle_launch",
    "VARIANT_EXPANSION": "variant_launch",
}

def _f(v):
    if v is None: return 0.0
    return float(v) if isinstance(v, Decimal) else float(v)


def main():
    conn = connect_acc(autocommit=False)
    cur = conn.cursor()

    # 1. Get accepted/completed opportunities
    cur.execute("""
        SELECT g.id, g.opportunity_type, g.marketplace_id, g.sku, g.asin,
               g.estimated_revenue_uplift, g.estimated_profit_uplift,
               g.estimated_margin_uplift, g.estimated_units_uplift,
               g.status, g.priority_score, g.confidence_score,
               g.created_at
        FROM growth_opportunity g
        WHERE g.status IN ('accepted', 'completed')
        ORDER BY g.created_at DESC
    """)
    opps = cur.fetchall()
    print(f"Found {len(opps)} accepted/completed opportunities")

    if not opps:
        # Create some synthetic executions from top-scoring new opportunities
        cur.execute("""
            SELECT TOP 40 g.id, g.opportunity_type, g.marketplace_id, g.sku, g.asin,
                   g.estimated_revenue_uplift, g.estimated_profit_uplift,
                   g.estimated_margin_uplift, g.estimated_units_uplift,
                   g.status, g.priority_score, g.confidence_score,
                   g.created_at
            FROM growth_opportunity g
            WHERE g.status = 'new' AND g.sku IS NOT NULL
            ORDER BY g.priority_score DESC
        """)
        opps = cur.fetchall()
        print(f"Using top {len(opps)} new opportunities for synthetic backfill")

    exec_count = 0
    outcome_count = 0

    for opp in opps:
        opp_id = opp[0]
        opp_type = opp[1]
        mkt = opp[2]
        sku = opp[3]
        asin = opp[4]
        est_rev = _f(opp[5])
        est_profit = _f(opp[6])
        est_margin = _f(opp[7])
        est_units = int(opp[8] or 0)
        prio_score = _f(opp[10])
        conf_score = _f(opp[11])
        created = opp[12]

        action = ACTION_MAP.get(opp_type, "action")
        windows = MONITORING_WINDOWS.get(opp_type, [14, 30])
        max_window = max(windows)

        # Build baseline — query profitability data or synthesize
        baseline = {}
        if sku:
            from_d = (date.today() - timedelta(days=60)).isoformat()
            to_d = (date.today() - timedelta(days=30)).isoformat()
            if mkt:
                cur.execute("""
                    SELECT SUM(revenue_pln), SUM(profit_pln), AVG(margin_pct),
                           SUM(units_sold), SUM(orders_count)
                    FROM acc_sku_profitability_rollup
                    WHERE sku = ? AND period_date BETWEEN ? AND ? AND marketplace_id = ?
                """, (sku, from_d, to_d, mkt))
            else:
                cur.execute("""
                    SELECT SUM(revenue_pln), SUM(profit_pln), AVG(margin_pct),
                           SUM(units_sold), SUM(orders_count)
                    FROM acc_sku_profitability_rollup
                    WHERE sku = ? AND period_date BETWEEN ? AND ?
                """, (sku, from_d, to_d))
            r = cur.fetchone()
            if r and r[0] is not None:
                baseline = {
                    "revenue_30d": _f(r[0]),
                    "profit_30d": _f(r[1]),
                    "margin_30d": _f(r[2]),
                    "units_30d": int(r[3] or 0),
                    "orders_30d": int(r[4] or 0),
                }

        if not baseline:
            baseline = {
                "revenue_30d": round(random.uniform(2000, 30000), 2),
                "profit_30d": round(random.uniform(200, 5000), 2),
                "margin_30d": round(random.uniform(5, 35), 2),
                "units_30d": random.randint(10, 200),
                "orders_30d": random.randint(5, 150),
            }

        expected = {
            "expected_revenue_delta": est_rev,
            "expected_profit_delta": est_profit,
            "expected_margin_delta": est_margin,
            "expected_units_delta": est_units,
        }

        # Synthetic execution date: 30-60 days ago
        exec_date = date.today() - timedelta(days=random.randint(30, 60))
        mon_end = exec_date + timedelta(days=max_window)

        entity_type = "sku" if sku else "asin" if asin else "marketplace"
        entity_id = sku or asin or mkt

        status = "evaluated" if mon_end <= date.today() else "monitoring"

        cur.execute("""
            INSERT INTO opportunity_execution
                (opportunity_id, entity_type, entity_id, action_type, executed_by,
                 baseline_metrics_json, expected_metrics_json,
                 monitoring_start, monitoring_end, status, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            opp_id, entity_type, entity_id, action, "backfill",
            json.dumps(baseline), json.dumps(expected),
            exec_date.isoformat(), mon_end.isoformat(), status, exec_date.isoformat()
        ))
        cur.execute("SELECT SCOPE_IDENTITY()")
        exec_id = int(cur.fetchone()[0])
        exec_count += 1

        # Create outcomes for each window (if enough time has elapsed)
        for window in windows:
            window_end = exec_date + timedelta(days=window)
            if window_end > date.today():
                continue

            # Simulate outcome with some realistic variance
            # success factor: varies around 1.0 with some noise
            success_factor = random.gauss(1.0, 0.35)
            success_factor = max(0.1, min(2.0, success_factor))

            actual_profit_delta = est_profit * success_factor * (window / 30) if est_profit else random.uniform(-200, 500)
            actual_revenue_delta = est_rev * success_factor * (window / 30) if est_rev else random.uniform(-500, 2000)

            actual = {
                "revenue_period": baseline.get("revenue_30d", 0) + actual_revenue_delta,
                "profit_period": baseline.get("profit_30d", 0) + actual_profit_delta,
                "margin_period": baseline.get("margin_30d", 0) + random.uniform(-3, 5),
                "units_period": baseline.get("units_30d", 0) + random.randint(-10, 30),
            }

            delta = {
                "revenue_delta": actual_revenue_delta,
                "profit_delta": actual_profit_delta,
                "margin_delta": actual["margin_period"] - baseline.get("margin_30d", 0),
                "units_delta": actual["units_period"] - baseline.get("units_30d", 0),
            }

            exp_profit_delta = est_profit * (window / 30) if est_profit else None
            if exp_profit_delta and exp_profit_delta > 0:
                success_score = round(actual_profit_delta / exp_profit_delta, 4)
            elif actual_profit_delta > 0:
                success_score = 1.5
            else:
                success_score = round(success_factor, 4)

            impact_score = min(100, max(0, round(actual_profit_delta / 500 * 100, 1)))

            # Confidence adjustment
            if success_score >= 1.2:
                conf_adj = 0.05
            elif success_score >= 0.8:
                conf_adj = 0.02
            elif success_score >= 0.4:
                conf_adj = -0.05
            else:
                conf_adj = -0.12

            eval_date = (window_end + timedelta(days=1)).isoformat()

            cur.execute("""
                INSERT INTO opportunity_outcome
                    (execution_id, monitoring_days, actual_metrics_json,
                     expected_metrics_json, delta_json,
                     success_score, impact_score, confidence_adjustment, evaluated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                exec_id, window,
                json.dumps({k: round(v, 2) for k, v in actual.items()}),
                json.dumps(expected),
                json.dumps({k: round(v, 2) for k, v in delta.items()}),
                success_score, impact_score, conf_adj, eval_date
            ))
            outcome_count += 1

    conn.commit()
    print(f"Created {exec_count} executions, {outcome_count} outcomes")

    # 2. Run learning aggregation
    print("\nRunning learning aggregation...")
    from app.services.decision_intelligence_service import run_learning_aggregation, run_model_recalibration
    result = run_learning_aggregation()
    print(f"Learning: {result}")

    # 3. Run model recalibration
    print("Running model recalibration...")
    result2 = run_model_recalibration()
    print(f"Recalibration: {result2}")

    # 4. Final counts
    cur2 = conn.cursor()
    for t in ['opportunity_execution', 'opportunity_outcome', 'decision_learning', 'opportunity_model_adjustments']:
        cur2.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {cur2.fetchone()[0]} rows")
    cur2.close()
    conn.close()
    print("\nBackfill complete!")

if __name__ == "__main__":
    main()
