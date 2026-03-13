"""Run Decision Intelligence migration on Azure SQL."""
import sys, os
sys.path.insert(0, r"C:\ACC\apps\api")
os.environ.setdefault("DATABASE_URL", "mssql+pymssql://acc-sql-kadax.database.windows.net/ACC")
from app.core.db_connection import connect_acc

SQL = r"""
-- 1) opportunity_execution
IF OBJECT_ID('dbo.opportunity_execution', 'U') IS NULL
CREATE TABLE dbo.opportunity_execution (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_id        INT           NOT NULL,
    entity_type           VARCHAR(30)   NULL,
    entity_id             VARCHAR(100)  NULL,
    action_type           VARCHAR(40)   NOT NULL,
    executed_by           VARCHAR(80)   NULL,
    executed_at           DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    baseline_metrics_json NVARCHAR(MAX) NULL,
    expected_metrics_json NVARCHAR(MAX) NULL,
    monitoring_start      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    monitoring_end        DATETIME2     NULL,
    status                VARCHAR(20)   NOT NULL DEFAULT 'monitoring'
);

-- 2) opportunity_outcome
IF OBJECT_ID('dbo.opportunity_outcome', 'U') IS NULL
CREATE TABLE dbo.opportunity_outcome (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    execution_id          INT           NOT NULL,
    monitoring_days       INT           NOT NULL,
    actual_metrics_json   NVARCHAR(MAX) NULL,
    expected_metrics_json NVARCHAR(MAX) NULL,
    delta_json            NVARCHAR(MAX) NULL,
    success_score         DECIMAL(8,4)  NULL,
    impact_score          DECIMAL(8,4)  NULL,
    confidence_adjustment DECIMAL(6,3)  NULL,
    evaluated_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 3) decision_learning
IF OBJECT_ID('dbo.decision_learning', 'U') IS NULL
CREATE TABLE dbo.decision_learning (
    id                    INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_type      VARCHAR(40)   NOT NULL,
    sample_size           INT           NOT NULL DEFAULT 0,
    avg_expected_profit   DECIMAL(18,2) NULL,
    avg_actual_profit     DECIMAL(18,2) NULL,
    prediction_accuracy   DECIMAL(8,4)  NULL,
    avg_success_score     DECIMAL(8,4)  NULL,
    confidence_adjustment DECIMAL(6,3)  NULL,
    win_rate              DECIMAL(8,4)  NULL,
    avg_roi               DECIMAL(8,4)  NULL,
    last_updated          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 4) opportunity_model_adjustments
IF OBJECT_ID('dbo.opportunity_model_adjustments', 'U') IS NULL
CREATE TABLE dbo.opportunity_model_adjustments (
    id                           INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_type             VARCHAR(40)   NOT NULL,
    impact_weight_adjustment     DECIMAL(6,3)  NOT NULL DEFAULT 0,
    confidence_weight_adjustment DECIMAL(6,3)  NOT NULL DEFAULT 0,
    priority_weight_adjustment   DECIMAL(6,3)  NOT NULL DEFAULT 0,
    reason                       NVARCHAR(500) NULL,
    updated_at                   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
"""

IDX = [
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_opp_exec_opp_id') CREATE NONCLUSTERED INDEX IX_opp_exec_opp_id ON dbo.opportunity_execution (opportunity_id);",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_opp_exec_status') CREATE NONCLUSTERED INDEX IX_opp_exec_status ON dbo.opportunity_execution (status, monitoring_end);",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_opp_outcome_exec') CREATE NONCLUSTERED INDEX IX_opp_outcome_exec ON dbo.opportunity_outcome (execution_id, monitoring_days);",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_decision_learning_type') CREATE NONCLUSTERED INDEX IX_decision_learning_type ON dbo.decision_learning (opportunity_type);",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_model_adj_type') CREATE NONCLUSTERED INDEX IX_model_adj_type ON dbo.opportunity_model_adjustments (opportunity_type);",
]

def main():
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    # Create tables (split by semicolons + GO-style execution)
    for block in SQL.split(";"):
        block = block.strip()
        if block and not block.startswith("--"):
            try:
                cur.execute(block)
                print(f"OK: {block[:60]}...")
            except Exception as e:
                print(f"SKIP (may already exist): {e}")

    # Create indexes
    for idx_sql in IDX:
        try:
            cur.execute(idx_sql)
            print(f"OK: {idx_sql[:60]}...")
        except Exception as e:
            print(f"IDX SKIP: {e}")

    # Verify
    cur.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME IN ('opportunity_execution','opportunity_outcome','decision_learning','opportunity_model_adjustments')
        ORDER BY TABLE_NAME
    """)
    tables = [r[0] for r in cur.fetchall()]
    print(f"\nTables found: {tables}")
    assert len(tables) == 4, f"Expected 4 tables, got {len(tables)}"
    print("Migration complete!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
