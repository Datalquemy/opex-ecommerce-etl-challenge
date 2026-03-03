"""
===============================================================================
LOAD LAYER
===============================================================================

Propósito:
-----------
Persistir en SQLite el modelo relacional (dim/fact) generado por transform.py
y ejecutar la consulta obligatoria (Top 3 usuarios por gasto total + país).

Entradas:
---------
- output/dim_users.csv
- output/fact_transactions.csv

Salidas:
--------
- output/ecommerce.db (SQLite)
- Prints con conteos + Top 3 result

Qué hace:
---------
1) Carga artefactos CSV.
2) Crea/conecta a SQLite (output/ecommerce.db).
3) FULL REFRESH: dropea y recrea tablas.
4) Inserta dim_users y fact_transactions.
5) Valida conteos.
6) Ejecuta query obligatoria (Top 3 por gasto), excluyendo amount NULL.
7) Imprime rutas de artefactos generados.

Notas:
------
- SQLite no tiene tipo DATE real; guardamos fechas normalizadas como TEXT ISO (YYYY-MM-DD).
- La lógica analítica (p.ej. excluir amount nulo) se aplica en la consulta SQL.
"""

import sqlite3
import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURACIÓN DE PATHS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DIM_USERS_CSV = OUTPUT_DIR / "dim_users.csv"
FACT_TX_CSV = OUTPUT_DIR / "fact_transactions.csv"
DB_PATH = OUTPUT_DIR / "ecommerce.db"


# =============================================================================
# EXTRACT (desde artefactos)
# =============================================================================

def load_artifacts() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carga dim_users y fact_transactions desde output/.
    """
    dim_users = pd.read_csv(DIM_USERS_CSV)
    fact_tx = pd.read_csv(FACT_TX_CSV)
    return dim_users, fact_tx


# =============================================================================
# DB (conexión y schema)
# =============================================================================

def connect_db() -> sqlite3.Connection:
    """
    Abre conexión a SQLite. Crea el archivo si no existe.
    """
    conn = sqlite3.connect(DB_PATH)
    return conn


def full_refresh_schema(conn: sqlite3.Connection) -> None:
    """
    FULL REFRESH: elimina tablas existentes y recrea el esquema con PK/FK.
    """
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    DROP TABLE IF EXISTS fact_transactions;
    DROP TABLE IF EXISTS dim_users;

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


# =============================================================================
# LOAD (insertar datos)
# =============================================================================

def prepare_dim_users(dim_users: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta tipos básicos para compatibilidad con SQLite (INTEGER/TEXT).
    """
    df = dim_users.copy()
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").astype("Int64")
    return df


def prepare_fact_transactions(fact_tx: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta columnas para el schema y tipos básicos.
    - Renombra date -> date_raw para conservar el campo original.
    """
    df = fact_tx.copy()

    if "date" in df.columns and "date_raw" not in df.columns:
        df = df.rename(columns={"date": "date_raw"})

    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce").astype("Int64")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    return df


def load_tables(conn: sqlite3.Connection, dim_users: pd.DataFrame, fact_tx: pd.DataFrame) -> None:
    """
    Inserta dim_users y fact_transactions en SQLite.
    """
    dim_users.to_sql("dim_users", conn, if_exists="append", index=False)
    fact_tx.to_sql("fact_transactions", conn, if_exists="append", index=False)


# =============================================================================
# VALIDACIÓN (conteos)
# =============================================================================

def get_table_counts(conn: sqlite3.Connection) -> dict:
    """
    Devuelve conteos de filas por tabla.
    """
    cur = conn.cursor()
    dim_count = cur.execute("SELECT COUNT(*) FROM dim_users;").fetchone()[0]
    fact_count = cur.execute("SELECT COUNT(*) FROM fact_transactions;").fetchone()[0]
    return {"dim_users": dim_count, "fact_transactions": fact_count}


# =============================================================================
# ANALYTICS (query obligatoria)
# =============================================================================

def query_top3_users(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Query obligatoria:
    Top 3 usuarios con mayor gasto total + país.
    Excluye amount nulo (amount IS NOT NULL).
    """
    sql = """
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
    return pd.read_sql_query(sql, conn)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=== LOAD LAYER START ===")
    print("Artifacts:")
    print(" -", DIM_USERS_CSV)
    print(" -", FACT_TX_CSV)

    dim_users, fact_tx = load_artifacts()
    print(f"Loaded artifacts ✅ | dim_users={dim_users.shape} | fact_transactions={fact_tx.shape}")

    conn = connect_db()
    try:
        full_refresh_schema(conn)
        print("Schema FULL REFRESH ✅")

        dim_users_sql = prepare_dim_users(dim_users)
        fact_sql = prepare_fact_transactions(fact_tx)

        load_tables(conn, dim_users_sql, fact_sql)
        counts = get_table_counts(conn)
        print("Loaded to SQLite ✅ | counts:", counts)

        top3 = query_top3_users(conn)
        print("\nTop 3 users by total spend (amount IS NOT NULL):")
        print(top3)

    finally:
        conn.close()

    print("\n=== LOAD LAYER COMPLETE ✅ ===")
    print("Generated artifacts:")
    print(" - ecommerce.db")
    print("\nOutput path:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()