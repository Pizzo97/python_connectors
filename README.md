# Luna 🚀

Piattaforma di Data Engineering e Analytics ad alte prestazioni per ambienti Python, progettata per garantire codice pulito, disciplinato ed esplicito per utenti in transizione dal mondo **SAS**.

---

## 🏛️ Struttura e Architettura del Progetto

Il progetto adotta il **`src-layout`** ufficiale PyPA per garantire massimo isolamento, zero conflitti di import e perfetta integrazione con `pip`:

```text
luna_connectors/                     # Radice del repository / workspace
├── .gitignore                       # File e cache temporanee ignorate
├── pyproject.toml                   # Configurazione pacchetto, dipendenze ed extra
├── README.md                        # Documentazione del progetto
└── src/
    └── luna/                        # Pacchetto Python installabile (import luna)
        ├── __init__.py              # __version__ dinamica, NullHandler e __all__ di guardia
        └── connectors/              # Gli 11 connettori sorgente
            ├── __init__.py          # __all__ = [] per bloccare wildcard imports
            ├── core/                # Motore interno di streaming (condiviso)
            │   ├── __init__.py
            │   ├── engine.py        # PyArrow streaming (fetch 50k, Row Group 128MB, cap 250k)
            │   └── models.py        # Dataclass ExtractionResult (zero dipendenze)
            │
            ├── oracle.py            # oracle_to_parquet, oracle_to_polars, oracle_to_pandas
            ├── postgres.py          # postgres_to_parquet, postgres_to_polars, ...
            ├── mssql.py             # mssql_to_parquet (turbodbc C++ Arrow), ...
            ├── sqlite.py            # sqlite_to_parquet, ...
            ├── sas.py               # sas_to_parquet (.sas7bdat, .xpt), ...
            ├── db2.py               # db2_to_parquet (turbodbc C++ Arrow), ...
            ├── mysql.py             # mysql_to_parquet, ...
            ├── teradata.py          # teradata_to_parquet (turbodbc C++ Arrow), ...
            ├── bigquery.py          # bigquery_to_parquet, ...
            ├── gcs.py               # gcs_to_parquet (DuckDB httpfs), ...
            └── dremio.py            # dremio_to_parquet (ADBC Flight SQL), ...
```

---

## 📦 Come si usa (Import Espliciti e Disciplinati)

Gli utenti importano solo la funzione specifica dal modulo dedicato. Ogni modulo SQL espone anche una funzione **`connect(...)`** per creare la connessione tramite la libreria ufficiale ottimizzata dalla piattaforma:

```python
# 1. Oracle: Autenticazione automatica (stile SAS - Luna gestisce apertura, estrazione e chiusura)
from luna.connectors.oracle import oracle_to_parquet

res = oracle_to_parquet(
    query="SELECT * FROM transazioni WHERE anno = 2024",
    target_path="transazioni.parquet",
    user="mario",
    password="secret_password",
    dsn="db_server:1521/service_name"
)
print(res)

# Oppure creando esplicitamente la connessione tramite la funzione connect() di Luna:
from luna.connectors.oracle import connect, oracle_to_parquet

conn = connect(user="mario", password="secret_password", dsn="db_server:1521/service_name")
oracle_to_parquet(query="SELECT * FROM transazioni", target_path="t1.parquet", connection=conn)
oracle_to_parquet(query="SELECT * FROM saldi", target_path="t2.parquet", connection=conn)
conn.close()

# 2. PostgreSQL
from luna.connectors.postgres import postgres_to_parquet

postgres_to_parquet(
    query="SELECT * FROM clienti",
    target_path="clienti.parquet",
    user="postgres",
    password="secret_password",
    database="crm",
    host="localhost"
)

# 3. Dataset SAS storici (.sas7bdat / .xpt)
from luna.connectors.sas import sas_to_parquet

sas_to_parquet(sas_file="clienti_storico.sas7bdat", target_path="clienti.parquet")

# 4. Google BigQuery (con credenziali/progetto o client già aperto)
from luna.connectors.bigquery import bigquery_to_parquet

bigquery_to_parquet(
    query="SELECT * FROM dataset.tabella",
    target_path="export.parquet",
    project="my-gcp-project"
)
```

👉 **Isolamento Totale:** Se un utente usa `oracle_to_parquet`, Python carica **esclusivamente `oracle.py`**. Non serve avere installato Teradata, BigQuery o altri driver non utilizzati.

---

## 🛡️ Standard Ingegneristici di Piattaforma

Tutti i parametri tecnici di scrittura Parquet sono governati internamente:
* **Database Fetch Size:** `50.000` righe (satura la banda TCP ed elimina i roundtrip di rete e l'overhead Python).
* **Target Row Group Size:** `128 MB` di memoria non compressa (misurata con `batch.nbytes`), con un tetto di **250.000 righe** (esattamente 5 fetch tondi) per garantire parallelismo multi-core su tabelle strette.
* **Compressione:** `Zstandard` (ZSTD) livello 3.
* **Data Page Size:** `1 MB`.
* **Dictionary Encoding & Statistiche min/max:** attivi di default.
* **Streaming Diretto:** Singolo passaggio PyArrow, **zero file temporanei**, RAM confinata (~130 MB max).

---

## ⚡ Analisi Dati Post-Estrazione

Una volta estratto il file Parquet, gli utenti possono analizzarlo out-of-core senza Pandas:

### Alternativa a `PROC SQL` (DuckDB):
```python
import duckdb

duckdb.sql("""
    SELECT 
        cod_filiale,
        count(*) AS num_operazioni,
        sum(importo) AS totale
    FROM 'transazioni.parquet'
    GROUP BY cod_filiale
    ORDER BY totale DESC
""").write_parquet("report_filiali.parquet")
```

### Alternativa a `DATA step` (Polars):
```python
import polars as pl

(
    pl.scan_parquet("transazioni.parquet")
    .filter(pl.col("importo") > 1000)
    .with_columns(
        commissione=pl.when(pl.col("importo") > 5000)
                      .then(pl.col("importo") * 0.02)
                      .otherwise(pl.col("importo") * 0.01)
    )
    .sink_parquet("transazioni_arricchite.parquet")
)
```
