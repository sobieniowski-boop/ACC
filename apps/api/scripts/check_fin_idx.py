import os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.chdir(r'C:\ACC\apps\api')
from dotenv import load_dotenv
load_dotenv(r'C:\ACC\.env')
from app.core.db_connection import connect_acc

c = connect_acc(timeout=30)
cur = c.cursor()
cur.execute("""
    SELECT i.name, i.type_desc
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID('dbo.acc_finance_transaction')
""")
for r in cur.fetchall():
    idx_name = r[0]
    idx_type = r[1]
    cur2 = c.cursor()
    cur2.execute("""
        SELECT COL_NAME(ic.object_id, ic.column_id)
        FROM sys.index_columns ic
        WHERE ic.object_id = OBJECT_ID('dbo.acc_finance_transaction')
          AND ic.index_id = (SELECT index_id FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.acc_finance_transaction') AND name = %s)
        ORDER BY ic.index_column_id
    """, (idx_name,))
    cols = [row[0] for row in cur2.fetchall()]
    print(f"{idx_name}: {idx_type} on ({', '.join(cols)})")
c.close()
