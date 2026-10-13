# Guida Configurazione ODBC su Linux per Luna (DB2, Teradata & MSSQL)

Questa guida documenta la configurazione del gestore **unixODBC** e dei driver ufficiali per **IBM DB2**, **Teradata Vantage** e **Microsoft SQL Server** su ambienti Linux (Debian/Ubuntu, RHEL/Rocky Linux o immagini Docker).

Luna utilizza **`turbodbc`** per questi database: questa libreria dialoga a livello C++ con il driver ODBC e trasferisce i dati **direttamente in memoria Apache Arrow** a piena velocità di rete, senza creare oggetti intermedi in Python.

---

## 1. Installazione del Gestore unixODBC

### Su Debian / Ubuntu / Docker (python:3.11-slim, ubuntu:22.04):
```bash
apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    libtool \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*
```

### Su RHEL / Rocky Linux / AlmaLinux:
```bash
dnf install -y \
    unixODBC \
    unixODBC-devel \
    curl
```

I due file di configurazione principali di sistema sono:
* `/etc/odbcinst.ini`: definisce i **Driver** installati (percorso delle librerie `.so`).
* `/etc/odbc.ini`: definisce i **DSN** di sistema (nomi di connessione).

---

## 2. Microsoft SQL Server (MSSQL): Installazione Driver

Microsoft pacchettizza ufficialmente **ODBC Driver 18 for SQL Server** (`msodbcsql18`) con registrazione automatica in `/etc/odbcinst.ini`.

### Su Debian / Ubuntu:
```bash
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -fsSL "https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list" | tee /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### Su RHEL / Rocky Linux:
```bash
curl -fsSL https://packages.microsoft.com/config/rhel/9/prod.repo | tee /etc/yum.repos.d/mssql-release.repo
ACCEPT_EULA=Y dnf install -y msodbcsql18
```

*Nota:* Il pacchetto Microsoft registra automaticamente il driver come `[ODBC Driver 18 for SQL Server]` in `/etc/odbcinst.ini`. Zero configurazione manuale dei file `.ini`.

---

## 3. IBM DB2: Installazione Driver e Configurazione

### Download del Driver
Scaricare da IBM il pacchetto **IBM Data Server Driver for ODBC and CLI** (es. `v11.5.8_linuxx64_odbc_cli.tar.gz`).

### Installazione
```bash
mkdir -p /opt/ibm
tar -xzf v11.5.8_linuxx64_odbc_cli.tar.gz -C /opt/ibm
# La cartella estratta sarà /opt/ibm/clidriver
```

### Registrazione Driver in `/etc/odbcinst.ini`
Aggiungere in `/etc/odbcinst.ini`:
```ini
[IBM DB2 ODBC DRIVER]
Description = IBM DB2 ODBC Driver
Driver      = /opt/ibm/clidriver/lib/libdb2o.so
FileUsage   = 1
```

### Configurazione Minima DSN in `/etc/odbc.ini`
Come da policy Luna, **NON inseriamo username, password o database specifico** nel file di sistema. Registriamo solo l'aggancio del DSN al driver installato:
```ini
[DB2]
Description = IBM DB2 Data Source
Driver      = IBM DB2 ODBC DRIVER
```
*Tutti i parametri di connessione (host, porta, database, credenziali) vengono passati dinamicamente tramite Luna da Python.*

---

## 4. Teradata Vantage: Installazione Driver e Configurazione

### Download dei Pacchetti Ufficiali
Dal portale Teradata scaricare il bundle Teradata Tools and Utilities (TTU) per Linux (pacchetti `.deb` o `.rpm`):
1. `tdicu`
2. `TeraGSS`
3. `tdodbc`

### Installazione (esempio Debian/Ubuntu)
```bash
dpkg -i tdicu_*.deb TeraGSS_*.deb tdodbc_*.deb
# I file vengono installati tipicamente sotto /opt/teradata/client/<version>/odbc_64
```

### Registrazione Driver in `/etc/odbcinst.ini`
Aggiungere in `/etc/odbcinst.ini`:
```ini
[Teradata Database ODBC Driver 17.20]
Description = Teradata Database ODBC Driver
Driver      = /opt/teradata/client/17.20/odbc_64/lib/tdataodbc_sb64.so
APILevel    = 2
ConnectFunctions = YYY
DriverODBCVer = 3.51
FileUsage   = 1
```

### Configurazione Minima DSN in `/etc/odbc.ini`
```ini
[TERADATA]
Description = Teradata Data Source
Driver      = Teradata Database ODBC Driver 17.20
```

---

## 5. Test di Verifica da Riga di Comando

Per verificare i driver e i DSN registrati su Linux:

```bash
# 1. Verifica che i driver siano visibili
odbcinst -q -d

# Output atteso:
# [ODBC Driver 18 for SQL Server]
# [IBM DB2 ODBC DRIVER]
# [Teradata Database ODBC Driver 17.20]

# 2. Verifica che i DSN minimi siano registrati
odbcinst -q -s
```

---

## 6. Utilizzo in Python con Luna

```python
# 1. Microsoft SQL Server
from luna.connectors.mssql import connect, mssql_to_parquet

mssql_to_parquet(
    query="SELECT * FROM vendite",
    target_path="vendite.parquet",
    server="sqlserver.azienda.lan",
    database="DWH",
    user="sa",
    password="secret_password"
)

# 2. IBM DB2
from luna.connectors.db2 import db2_to_parquet

db2_to_parquet(
    query="SELECT * FROM transazioni",
    target_path="transazioni.parquet",
    dsn="DB2",
    host="db2.azienda.lan",
    port=50000,
    database="FINANZA",
    user="mario",
    password="secret_password"
)
```
