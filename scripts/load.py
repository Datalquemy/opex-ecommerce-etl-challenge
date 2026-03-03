# %%
"""
LOAD LAYER (Notebook-style): CSV artifacts -> SQLite (output/ecommerce.db)

Propósito:
- Crear un modelo relacional simple en SQLite.
- Cargar dim_users y fact_transactions generadas por transform.py.
- Ejecutar la query obligatoria: Top 3 usuarios por gasto total + país.

Entradas:
- output/dim_users.csv
- output/fact_transactions.csv

Salida:
- output/ecommerce.db (SQLite)
- prints con conteos + Top 3 result

Notas:
- En este reto usaremos FULL REFRESH: borra/crea tablas cada ejecución.
- En análisis SQL, excluimos amount nulo (amount IS NOT NULL).
"""

# %%
# [CELDA 1] Imports + Paths
import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DIM_USERS_CSV = OUTPUT_DIR / "dim_users.csv"
FACT_TX_CSV = OUTPUT_DIR / "fact_transactions.csv"
DB_PATH = OUTPUT_DIR / "ecommerce.db"

print("DIM_USERS_CSV:", DIM_USERS_CSV)
print("FACT_TX_CSV:", FACT_TX_CSV)
print("DB_PATH:", DB_PATH)

# %%
# [CELDA 2] Cargar artefactos CSV (output)
dim_users = pd.read_csv(DIM_USERS_CSV)
fact_tx = pd.read_csv(FACT_TX_CSV)

print("dim_users shape:", dim_users.shape)
print("fact_transactions shape:", fact_tx.shape)

print("dim_users columns:", dim_users.columns.tolist())
print("fact_transactions columns:", fact_tx.columns.tolist())

# %%
# [CELDA 3] Sanity checks rápidos
print("dim_users user_id nulls:", int(dim_users["user_id"].isna().sum()))
print("fact_tx transaction_id nulls:", int(fact_tx["transaction_id"].isna().sum()))
print("fact_tx user_id nulls:", int(fact_tx["user_id"].isna().sum()))

print("fact_tx amount nulls:", int(fact_tx["amount"].isna().sum()))
print("fact_tx date_status counts:\n", fact_tx["date_status"].value_counts())

# %%
# [CELDA 4] Conectar a SQLite (crea DB si no existe)
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
print("Connected to SQLite ✅")

# %%
# [CELDA 5] FULL REFRESH: drop tables (si existen)
cur.executescript("""
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_users;
""")
conn.commit()
print("Dropped existing tables ✅")

# %%
# [CELDA 6] Crear esquema relacional (PK/FK)
# Nota: SQLite aplica FK solo si PRAGMA foreign_keys = ON.
cur.executescript("""
PRAGMA foreign_keys = ON;

CREATE TABLE dim_users (
    user_id     INTEGER PRIMARY KEY,
    user_name   TEXT,
    country     TEXT,
    email       TEXT,
    signup_date TEXT
);

CREATE TABLE fact_transactions (
    transaction_id   TEXT PRIMARY KEY,
    user_id          INTEGER,
    product_category TEXT,
    amount           REAL,
    date_raw         TEXT,
    transaction_date TEXT,
    date_status      TEXT,
    country          TEXT,
    FOREIGN KEY (user_id) REFERENCES dim_users(user_id)
);
""")
conn.commit()
print("Created tables ✅")

# %%
# [CELDA 7] Cargar dim_users a SQLite
# Tip: aseguramos tipos compatibles con SQLite
dim_users_sql = dim_users.copy()
dim_users_sql["user_id"] = pd.to_numeric(dim_users_sql["user_id"], errors="coerce").astype("Int64")
# signup_date ya viene como string en CSV; lo dejamos en TEXT

dim_users_sql.to_sql("dim_users", conn, if_exists="append", index=False)
print("Loaded dim_users ✅")

# %%
# [CELDA 8] Cargar fact_transactions a SQLite
fact_sql = fact_tx.copy()

# Renombrar columnas para que coincidan con el schema
fact_sql = fact_sql.rename(columns={"date": "date_raw"})

# Asegurar tipos
fact_sql["user_id"] = pd.to_numeric(fact_sql["user_id"], errors="coerce").astype("Int64")
fact_sql["amount"] = pd.to_numeric(fact_sql["amount"], errors="coerce")

fact_sql.to_sql("fact_transactions", conn, if_exists="append", index=False)
print("Loaded fact_transactions ✅")

# %%
# [CELDA 9] Validar conteos en DB (rows)
dim_count = cur.execute("SELECT COUNT(*) FROM dim_users;").fetchone()[0]
fact_count = cur.execute("SELECT COUNT(*) FROM fact_transactions;").fetchone()[0]
print("DB dim_users rows:", dim_count)
print("DB fact_transactions rows:", fact_count)

# %%
# [CELDA 10] Query obligatoria: Top 3 usuarios por gasto total + país
# Importante: excluimos amount NULL
query_top3 = """
SELECT
    u.user_id,
    u.user_name,
    u.country,
    ROUND(SUM(f.amount), 2) AS total_spent
FROM fact_transactions f
JOIN dim_users u
  ON f.user_id = u.user_id
WHERE f.amount IS NOT NULL
GROUP BY u.user_id, u.user_name, u.country
ORDER BY total_spent DESC
LIMIT 3;
"""

top3 = pd.read_sql_query(query_top3, conn)
print("\nTop 3 users by total spend:")
print(top3)

# %%
# [CELDA 11] Cerrar conexión
conn.close()
print("\nSQLite DB ready ✅")
print("DB path:", DB_PATH.resolve())