from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'acc_product'
    ORDER BY ORDINAL_POSITION
""")
rows = cur.fetchall()
for r in rows:
    print(f"{r[0]}: {r[1]}")
conn.close()
