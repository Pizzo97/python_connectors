# Luna Project Rules & Engineering Guidelines

## 🎯 Core Directive: Exact Execution, Zero Over-Engineering

1. **Keep It Simple & Linear (KISS)**
   - Scrivi codice Python pulito, piatto ed esplicito.
   - Evita astrazioni premature: non usare decoratori custom, metaclassi, factory complesse o import dinamici se un blocco lineare (`if/else`, `try/finally`) risolve il problema in modo chiaro.

2. **Un Solo Percorso di Esecuzione (Zero Scorciatoie Nascoste)**
   - Ogni connettore ha **un solo flusso standard**: `stream_cursor_to_parquet`.
   - MAI introdurre motori alternativi nascosti (es. scorciatoie con DuckDB `ATTACH` dentro connettori SQLite o simili).
   - MAI nascondere o ingoiare eccezioni con blocchi muti (`except Exception: pass`). Gli errori devono emergere chiaramente.

3. **Gestione Risorse Chiara ed Esplicita**
   - Apri cursori e connessioni in modo trasparente.
   - Chiudi sempre le risorse create localmente con blocchi `try...finally`.
   - Non chiudere connessioni fornite direttamente dall'utente.

4. **Nessuna Magia negli `__init__.py`**
   - I file `__init__.py` devono rimanere minimali: versione, `NullHandler` di logging, ed eventuale lista `__all__` per bloccare wildcard imports.
   - Zero `__getattr__` magici, zero lazy-import acrobatici. Gli import devono essere sempre espliciti (`from luna.connectors.oracle import oracle_to_parquet`).

5. **Firme API Pulite e Stabili**
   - Mantieni le firme focalizzate sui parametri di business (`connection`, `query`, `target_path`).
   - I parametri tecnici e le costanti di streaming (dimensione fetch, compressione ZSTD, row group 128MB/250k) restano rigidamente governati dentro `core/engine.py`.
