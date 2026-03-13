"""Quick test: CompatCursor handles both pyodbc calling patterns."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# Pattern 1: sequence
cur.execute("SELECT ? AS col1", ["hello"])
print("Pattern 1 (list):", cur.fetchone()[0])

# Pattern 2: multiple args (pyodbc-style)
cur.execute("SELECT ? AS col1, ? AS col2", "a", "b")
row = cur.fetchone()
print("Pattern 2 (args):", row[0], row[1])

# Pattern 3: many args (like backfill INSERT with 13 params)
cur.execute(
    "SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?",
    "p1","p2","p3","p4","p5","p6","p7","p8","p9","p10","p11","p12","p13"
)
row = cur.fetchone()
print(f"Pattern 3 (13 args): {len(row)} cols -> OK")

conn.close()
print("All CompatCursor patterns working!")
