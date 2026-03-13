# Azure SQL Free Tier — Poradnik Konfiguracji

> Instrukcje krok po kroku: od utworzenia konta Azure do działającej bazy danych ACC.

## 1. Utwórz konto Azure (bezpłatne)

1. Otwórz https://azure.microsoft.com/free/
2. Kliknij **Start free** / **Rozpocznij bezpłatnie**
3. Zaloguj się kontem Microsoft (lub utwórz nowe)
4. Podaj kartę płatniczą (weryfikacja tożsamości — **NIE zostanie obciążona**)
5. Po weryfikacji → Azure Portal: https://portal.azure.com

> **Nota:** Azure Free Tier daje 12 miesięcy bezpłatnych usług + $200 kredytu na 30 dni.

---

## 2. Utwórz serwer SQL

1. W portalu Azure → wyszukaj **SQL servers** → **+ Create**
2. Wypełnij:
   - **Subscription:** Twoja subskrypcja (Free Trial lub Pay-As-You-Go)
   - **Resource group:** kliknij **Create new** → wpisz `acc-rg`
   - **Server name:** `acc-sql-kadax` (będzie: `acc-sql-kadax.database.windows.net`)
   - **Location:** `(Europe) West Europe` lub `(Europe) Poland Central`
   - **Authentication:** wybierz **Use SQL authentication**
     - **Admin login:** `accadmin`
     - **Password:** wygeneruj silne hasło (zanotuj!)
3. Kliknij **Review + create** → **Create**
4. Poczekaj ~2 min na deployment

---

## 3. Utwórz bazę danych (Free Tier)

1. Po utworzeniu serwera → **Go to resource**
2. Na stronie serwera → **+ Create database** (u góry)
3. Wypełnij:
   - **Database name:** `ACC`
   - **Workload environment:** Development
   - **Compute + storage:** kliknij **Configure database**
     - Wybierz **Free** tier (32 GB, vCore)
     - **⚠️ WAŻNE:** upewnij się, że widisz "Free monthly limit: 100,000 vCore seconds"
   - **Backup storage redundancy:** Locally-redundant
4. Kliknij **Review + create** → **Create**
5. Poczekaj ~3-5 min

---

## 4. Otwórz firewall

1. Wejdź w serwer SQL → **Networking** (lewe menu)
2. Włącz **Allow Azure services and resources to access this server** → ON
3. Kliknij **+ Add your client IPv4 address** (doda twoje IP automatycznie)
   - Jeśli masz dynamiczne IP → dodaj zakres (np. `0.0.0.0` – `255.255.255.255` na czas dev)
4. Kliknij **Save**

---

## 5. Utwórz tabele ACC

Użyj gotowego skryptu z repozytorium:

### Opcja A: Azure Portal (Query editor)
1. Wejdź w bazę `ACC` → **Query editor (preview)** (lewe menu)
2. Zaloguj się: `accadmin` + hasło
3. Otwórz plik `scripts/azure_create_tables.sql` z repozytorium
4. Skopiuj i wklej do edytora → **Run**
5. Powinno wyświetlić "Query succeeded" (34 tabele + seed marketplace)

### Opcja B: Azure Data Studio / SSMS (jeśli masz)
```
sqlcmd -S acc-sql-kadax.database.windows.net -d ACC -U accadmin -P 'TWOJE_HASLO' -i scripts/azure_create_tables.sql
```

---

## 6. Zaktualizuj .env

Otwórz `C:\ACC\.env` i **dodaj / zaktualizuj** te zmienne:

```env
# ═══ ACC Own Database (Azure SQL) ═══
MSSQL_SERVER=acc-sql-kadax.database.windows.net
MSSQL_PORT=1433
MSSQL_USER=accadmin
MSSQL_PASSWORD=TWOJE_SILNE_HASLO
MSSQL_DATABASE=ACC

# ═══ Netfox ERP (read-only, stary serwer) ═══
NETFOX_MSSQL_SERVER=192.168.230.120
NETFOX_MSSQL_PORT=11901
NETFOX_MSSQL_USER=Analityka
NETFOX_MSSQL_PASSWORD=tE4rYuGmcU@@#$3
NETFOX_MSSQL_DATABASE=NetfoxAnalityka
```

> **WAŻNE:** `MSSQL_SERVER` z `database.windows.net` → system automatycznie użyje `pymssql` (TLS 1.2).

---

## 7. Przetestuj połączenie

```powershell
cd C:\ACC\apps\api
& ..\..\..venv\Scripts\python.exe -c "
from app.core.db_connection import connect_acc, connect_netfox

# Test Azure SQL
conn = connect_acc()
cur = conn.cursor()
cur.execute('SELECT @@VERSION')
print('Azure SQL:', cur.fetchone()[0][:80])
cur.close()
conn.close()

# Test Netfox ERP
conn2 = connect_netfox()
cur2 = conn2.cursor()
cur2.execute('SELECT TOP 1 1 FROM dbo.Kartoteki')
print('Netfox ERP: OK')
cur2.close()
conn2.close()
"
```

---

## 8. Migruj dane (opcjonalnie)

Jeśli chcesz przenieść istniejące dane z NetfoxAnalityka → Azure SQL:

```powershell
cd C:\ACC
& .venv\Scripts\python.exe scripts/migrate_to_azure.py
```

Skrypt:
- Kopiuje dane z 34 tabel `acc_*` + family mapper
- Pomija tabele, które już mają dane
- Batche po 100 rekordów
- **NIE modyfikuje** źródłowej bazy (read-only)

---

## 9. Uruchom backend

```powershell
cd C:\ACC\apps\api
& ..\..\..venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Dashboard powinien działać na http://localhost:3010

---

## FAQ

### Ile kosztuje Azure SQL Free Tier?
**$0 przez 12 miesięcy.** 32 GB storage, 100k vCore-seconds/miesiąc. Wystarczy na ACC.

### Co jak skończą się vCore-sekundy?
Baza zostanie wstrzymana (pauza). Reset limitu 1-go każdego miesiąca. Można też upgrade do Basic ($5/m).

### Czy muszę mieć ODBC Driver 17/18?
**NIE.** System używa `pymssql` do Azure SQL (TLS 1.2 bez ODBC). Stary driver "SQL Server" wystarczy do Netfox ERP.

### Co z backup?
Azure SQL Free Tier ma automatyczny backup (7 dni retention, point-in-time restore).

### Mogę wrócić do starego setupu?
Tak — wystarczy w `.env` ustawić `MSSQL_SERVER=192.168.230.120` i usunąć `NETFOX_MSSQL_*`. Wszystko wróci do starego trybu.
