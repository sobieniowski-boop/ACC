import sys
try:
    import pymssql
    import os
    from dotenv import load_dotenv
    load_dotenv('C:/ACC/.env')
    srv = os.getenv('MSSQL_SERVER')
    usr = os.getenv('MSSQL_USER')
    pwd = os.getenv('MSSQL_PASSWORD')
    db = os.getenv('MSSQL_DATABASE')
    with open('C:/ACC/dbtest.txt', 'w') as f:
        f.write(f'srv={srv} usr={usr} db={db}\n')
        try:
            c = pymssql.connect(server=srv, user=usr, password=pwd, database=db, port=1433, tds_version='7.3', login_timeout=10)
            cur = c.cursor()
            cur.execute('SELECT MIN(posted_date), MAX(posted_date), COUNT(*) FROM acc_finance_transaction')
            r = cur.fetchone()
            f.write(f'Range: {r[0]} to {r[1]}\nTotal: {r[2]}\n')
            cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
            r2 = cur.fetchone()
            f.write(f'FBAStorageFee: cnt={r2[0]} sum={r2[1]}\n')
            c.close()
            f.write('OK\n')
        except Exception as e:
            f.write(f'DB ERROR: {e}\n')
except Exception as e:
    with open('C:/ACC/dbtest.txt', 'w') as f:
        f.write(f'INIT ERROR: {e}\n')
