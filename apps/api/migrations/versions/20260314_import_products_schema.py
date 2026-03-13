"""Import products table (acc_import_products).

Revision ID: eb020
Revises: eb019
Create Date: 2026-03-14
"""
from alembic import op

revision = "eb020"
down_revision = "eb019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
IF OBJECT_ID('dbo.acc_import_products', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_import_products (
        id               INT IDENTITY(1,1) PRIMARY KEY,
        sku              NVARCHAR(120)  NOT NULL,
        nazwa_pelna      NVARCHAR(500)  NULL,
        kod_k            NVARCHAR(120)  NULL,
        kod_importu      NVARCHAR(120)  NULL,
        aktywny          BIT            NULL,
        data_pierwszej_dostawy  DATE    NULL,
        stan_magazynowy  INT            NULL,
        w_tym_fba        INT            NULL,
        sprzedaz_30d     INT            NULL,
        amazon_30d       INT            NULL,
        fba_30d          INT            NULL,
        allegro_30d      INT            NULL,
        sklep_30d        INT            NULL,
        inne_30d         INT            NULL,
        zasieg_dni       INT            NULL,
        estymacja_braku_stanu  DATE     NULL,
        dynamika_10_30   FLOAT          NULL,
        data_ostatniej_dostawy DATE     NULL,
        ilosc_ostatniej_dostawy INT     NULL,
        cena_zakupu      DECIMAL(18,4)  NULL,
        wartosc_magazynu DECIMAL(18,2)  NULL,
        srednia_cena_sprzedazy_30d DECIMAL(18,4) NULL,
        srednia_marza    DECIMAL(18,4)  NULL,
        marza            DECIMAL(18,4)  NULL,
        miejsc_paletowych FLOAT         NULL,
        koszt_skladowania_1szt_30d DECIMAL(18,4) NULL,
        koszt_skladowania_zapasu_30d DECIMAL(18,2) NULL,
        nasycenie_12m    FLOAT          NULL,
        data_dostawy     DATE           NULL,
        tempo_pokrycie_150d INT         NULL,
        sprzedaz_12m     INT            NULL,
        filtr            NVARCHAR(120)  NULL,
        mix              NVARCHAR(120)  NULL,
        is_import        BIT            NOT NULL DEFAULT 1,
        uploaded_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_import_products_sku ON dbo.acc_import_products(sku);
    CREATE INDEX IX_acc_import_products_import ON dbo.acc_import_products(is_import);
    CREATE INDEX IX_acc_import_products_kod ON dbo.acc_import_products(kod_importu);
END
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dbo.acc_import_products")
