"""Run strategy DB migration."""
import sys, os
sys.path.insert(0, ".")
os.chdir(r"C:\ACC\apps\api")

from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# --- Tables ---
cur.execute("""
IF OBJECT_ID('dbo.growth_opportunity', 'U') IS NULL
CREATE TABLE dbo.growth_opportunity (
    id                       INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_type         VARCHAR(40)   NOT NULL,
    marketplace_id           VARCHAR(30)   NULL,
    sku                      VARCHAR(100)  NULL,
    asin                     VARCHAR(20)   NULL,
    parent_asin              VARCHAR(20)   NULL,
    family_id                INT           NULL,
    title                    NVARCHAR(300) NOT NULL,
    description              NVARCHAR(MAX) NULL,
    root_cause               VARCHAR(40)   NULL,
    recommendation           NVARCHAR(MAX) NULL,
    priority_score           DECIMAL(5,1)  NOT NULL DEFAULT 0,
    confidence_score         DECIMAL(5,1)  NOT NULL DEFAULT 0,
    estimated_revenue_uplift DECIMAL(18,2) NULL,
    estimated_profit_uplift  DECIMAL(18,2) NULL,
    estimated_margin_uplift  DECIMAL(8,2)  NULL,
    estimated_units_uplift   INT           NULL,
    effort_score             DECIMAL(5,1)  NULL,
    owner_role               VARCHAR(40)   NULL,
    blocker_json             NVARCHAR(MAX) NULL,
    source_signals_json      NVARCHAR(MAX) NULL,
    status                   VARCHAR(20)   NOT NULL DEFAULT 'new',
    created_at               DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at               DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
)
""")
print("TABLE growth_opportunity OK")

cur.execute("""
IF OBJECT_ID('dbo.strategy_experiment', 'U') IS NULL
CREATE TABLE dbo.strategy_experiment (
    id                INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_id    INT           NULL,
    experiment_type   VARCHAR(40)   NOT NULL,
    marketplace_id    VARCHAR(30)   NULL,
    sku               VARCHAR(100)  NULL,
    asin              VARCHAR(20)   NULL,
    hypothesis        NVARCHAR(500) NOT NULL,
    owner             VARCHAR(80)   NULL,
    status            VARCHAR(20)   NOT NULL DEFAULT 'planned',
    start_date        DATE          NULL,
    end_date          DATE          NULL,
    success_metric    VARCHAR(100)  NULL,
    baseline_value    DECIMAL(18,2) NULL,
    result_value      DECIMAL(18,2) NULL,
    result_summary    NVARCHAR(MAX) NULL,
    created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
)
""")
print("TABLE strategy_experiment OK")

cur.execute("""
IF OBJECT_ID('dbo.growth_opportunity_log', 'U') IS NULL
CREATE TABLE dbo.growth_opportunity_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    opportunity_id  INT           NOT NULL,
    action          VARCHAR(30)   NOT NULL,
    actor           VARCHAR(80)   NULL,
    note            NVARCHAR(MAX) NULL,
    created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
)
""")
print("TABLE growth_opportunity_log OK")
conn.commit()

# --- Indexes ---
indexes = [
    ("IX_growth_opp_status_priority", "growth_opportunity",
     "CREATE NONCLUSTERED INDEX IX_growth_opp_status_priority ON dbo.growth_opportunity (status, priority_score DESC)"),
    ("IX_growth_opp_type", "growth_opportunity",
     "CREATE NONCLUSTERED INDEX IX_growth_opp_type ON dbo.growth_opportunity (opportunity_type)"),
    ("IX_growth_opp_sku", "growth_opportunity",
     "CREATE NONCLUSTERED INDEX IX_growth_opp_sku ON dbo.growth_opportunity (sku)"),
    ("IX_strategy_exp_status", "strategy_experiment",
     "CREATE NONCLUSTERED INDEX IX_strategy_exp_status ON dbo.strategy_experiment (status)"),
    ("IX_growth_log_opp", "growth_opportunity_log",
     "CREATE NONCLUSTERED INDEX IX_growth_log_opp ON dbo.growth_opportunity_log (opportunity_id, created_at)"),
]

for ix_name, tbl, ddl in indexes:
    try:
        cur.execute(ddl)
        print(f"INDEX {ix_name} created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"INDEX {ix_name} exists")
        else:
            print(f"INDEX {ix_name} ERROR: {e}")
conn.commit()

cur.execute("SELECT COUNT(*) FROM growth_opportunity")
print(f"growth_opportunity rows: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM strategy_experiment")
print(f"strategy_experiment rows: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM growth_opportunity_log")
print(f"growth_opportunity_log rows: {cur.fetchone()[0]}")
print("Migration DONE")
conn.close()
