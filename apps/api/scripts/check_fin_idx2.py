import os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.chdir(r'C:\ACC\apps\api')
from dotenv import load_dotenv
load_dotenv(r'C:\ACC\.env')
from app.core.db_connection import connect_acc

c = connect_acc(timeout=30)
cur = c.cursor()
cur.execute("""
    SELECT i.name, i.type_desc, ic.index_column_id, COL_NAME(ic.object_id, ic.column_id) AS col, ic.is_included_column
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE i.object_id = OBJECT_ID('dbo.acc_finance_transaction')
    ORDER BY i.name, ic.index_column_id
""")
from collections import defaultdict
idxs = defaultdict(lambda: {'type': '', 'key': [], 'inc': []})
for r in cur.fetchall():
    d = idxs[r[0]]
    d['type'] = r[1]
    if r[4]:
        d['inc'].append(r[3])
    else:
        d['key'].append(r[3])
for name, d in idxs.items():
    inc_str = f" INCLUDE({', '.join(d['inc'])})" if d['inc'] else ""
    print(f"  {name}: {d['type']} ({', '.join(d['key'])}){inc_str}")
c.close()
