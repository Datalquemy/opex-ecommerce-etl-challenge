"""
===============================================================================
TRANSFORM LAYER
===============================================================================

Propósito:
-----------
Este script implementa la capa de transformación del pipeline ETL.

Fuentes:
- data/transactions.csv
- data/user_data.json

Qué hace:
---------
1. Carga y tipa datos de origen.
2. Aplana metadata de usuarios (JSON -> columnas).
3. Normaliza transacciones (IDs, amount, fechas).
4. Enriquce transacciones con atributos de usuario.
5. Construye:
   - dim_users
   - fact_transactions
6. Genera artefactos:
   - output/dim_users.csv
   - output/fact_transactions.csv
   - output/transform_report.txt

Notas:
------
- No elimina registros por reglas de negocio (ej. amount nulo).
- La exclusión analítica se hará en la capa SQL.
- Las fechas se normalizan detectando DMY/YMD y separadores mixtos.
"""

import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURACIÓN DE PATHS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# EXTRACT
# =============================================================================

def load_sources():
    """
    Carga las fuentes crudas.
    - Fuerza 'date' como string para evitar inferencias inconsistentes.
    """
    transactions = pd.read_csv(
        DATA_DIR / "transactions.csv",
        dtype={"date": "string"}
    )
    users_raw = pd.read_json(DATA_DIR / "user_data.json")

    return transactions, users_raw


# =============================================================================
# TRANSFORM - USERS
# =============================================================================

def build_dim_users(users_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Aplana metadata del JSON de usuarios y tipa columnas clave.
    """

    users_meta = pd.json_normalize(users_raw["metadata"])

    users = pd.concat(
        [
            users_raw.drop(columns=["metadata"]).reset_index(drop=True),
            users_meta.reset_index(drop=True),
        ],
        axis=1,
    )

    users = users.rename(columns={"name": "user_name"})

    users["user_id"] = pd.to_numeric(users["user_id"], errors="coerce").astype("Int64")
    users["signup_date"] = pd.to_datetime(users["signup_date"], errors="coerce")

    dim_users = users[["user_id", "user_name", "country", "email", "signup_date"]].copy()

    return dim_users


# =============================================================================
# TRANSFORM - TRANSACTIONS (TIPADO BÁSICO)
# =============================================================================

def normalize_transaction_columns(tx: pd.DataFrame) -> pd.DataFrame:
    """
    Tipado estructural de transacciones.
    No aplica reglas de negocio.
    """

    tx["transaction_id"] = tx["transaction_id"].astype("string").str.strip()
    tx["user_id"] = pd.to_numeric(tx["user_id"], errors="coerce").astype("Int64")
    tx["product_category"] = tx["product_category"].astype("string").str.strip()
    tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce")

    return tx


# =============================================================================
# TRANSFORM - FECHAS (NORMALIZACIÓN ROBUSTA)
# =============================================================================

def normalize_dates(tx: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza fechas detectando:
    - DMY: 25/01/2024
    - YMD: 2024-01-25 o 2024/01/25
    Maneja mezcla de separadores.
    """

    s = tx["date"].astype("string").fillna("").str.strip()
    s = s.str.slice(0, 10)  # elimina timestamps
    s_norm = s.str.replace("-", "/", regex=False)

    mask_missing = s_norm.eq("")
    mask_dmy = s_norm.str.match(r"^\d{2}/\d{2}/\d{4}$")
    mask_ymd = s_norm.str.match(r"^\d{4}/\d{2}/\d{2}$")

    tx["transaction_date"] = pd.NaT

    tx.loc[mask_dmy, "transaction_date"] = pd.to_datetime(
        s_norm.where(mask_dmy),
        format="%d/%m/%Y",
        errors="coerce",
    )

    tx.loc[mask_ymd, "transaction_date"] = pd.to_datetime(
        s_norm.where(mask_ymd),
        format="%Y/%m/%d",
        errors="coerce",
    )

    tx["date_status"] = "OK"
    tx.loc[mask_missing, "date_status"] = "MISSING"
    tx.loc[(~mask_missing) & (tx["transaction_date"].isna()), "date_status"] = "UNPARSEABLE"

    return tx


# =============================================================================
# ENRICH - JOIN USERS
# =============================================================================

def enrich_transactions(tx: pd.DataFrame, dim_users: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza LEFT JOIN por user_id.
    Garantiza integridad many_to_one.
    """

    df = tx.merge(
        dim_users,
        on="user_id",
        how="left",
        validate="many_to_one",
    )

    return df


# =============================================================================
# BUILD FACT TABLE
# =============================================================================

def build_fact_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla de hechos final.
    """

    fact = df[
        [
            "transaction_id",
            "user_id",
            "product_category",
            "amount",
            "date",
            "transaction_date",
            "date_status",
            "country",
        ]
    ].copy()

    return fact


# =============================================================================
# OUTPUTS
# =============================================================================

def write_outputs(dim_users: pd.DataFrame, fact: pd.DataFrame, tx: pd.DataFrame):
    """
    Escribe artefactos y reporte de transformación.
    """

    dim_users.to_csv(OUTPUT_DIR / "dim_users.csv", index=False)
    fact.to_csv(OUTPUT_DIR / "fact_transactions.csv", index=False)

    report_lines = []
    report_lines.append("TRANSFORM REPORT\n")
    report_lines.append("=" * 60 + "\n")
    report_lines.append(f"dim_users rows: {len(dim_users)}\n")
    report_lines.append(f"fact_transactions rows: {len(fact)}\n\n")

    report_lines.append("DATE STATUS COUNTS\n")
    report_lines.append("-" * 60 + "\n")
    report_lines.append(str(tx["date_status"].value_counts().to_dict()) + "\n\n")

    report_lines.append("NULLS\n")
    report_lines.append("-" * 60 + "\n")
    report_lines.append(f"amount nulls: {int(tx['amount'].isna().sum())}\n")

    (OUTPUT_DIR / "transform_report.txt").write_text(
        "".join(report_lines),
        encoding="utf-8",
    )


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=== TRANSFORM LAYER START ===")

    tx, users_raw = load_sources()

    dim_users = build_dim_users(users_raw)

    tx = normalize_transaction_columns(tx)
    tx = normalize_dates(tx)

    df = enrich_transactions(tx, dim_users)

    fact = build_fact_transactions(df)

    write_outputs(dim_users, fact, tx)

    print("=== TRANSFORM LAYER COMPLETE ===") 
    print("\nGenerated artifacts:")
    print(f" - dim_users.csv")
    print(f" - fact_transactions.csv")
    print(f" - transform_report.txt")
    print(f"\nOutput path: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
