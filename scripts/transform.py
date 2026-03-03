# %%
"""
Transform Layer (Notebook-style): users_data.json + transactions.csv -> clean dims/facts + outputs

Objetivo:
- Aplanar users_data.json (metadata.*)
- Tipar y normalizar transacciones (IDs, amount, date)
- Enriquecer transacciones con atributos de usuario (country, email, name)
- Generar:
  - dim_users
  - fact_transactions (con date_status y transaction_date)
  - reportes/artefactos en output/

Notas:
- Este archivo es intencionalmente "celda por celda" para entender el flujo.
- Luego lo convertimos a módulos (extract/transform/load) como pide el reto.
"""

# %%
# [CELDA 1] Imports + Paths (config base del script)
import re
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("BASE_DIR:", BASE_DIR)
print("DATA_DIR:", DATA_DIR)
print("OUTPUT_DIR:", OUTPUT_DIR)

# %%
# [CELDA 2] Cargar fuentes (leer CSV con date como string para evitar inferencias raras)
transactions_path = DATA_DIR / "transactions.csv"
users_path = DATA_DIR / "user_data.json"

tx = pd.read_csv(transactions_path, dtype={"date": "string"})
users_raw = pd.read_json(users_path)

print("transactions shape:", tx.shape)
print("users_raw shape:", users_raw.shape)
print("transactions columns:", tx.columns.tolist())
print("users_raw columns:", users_raw.columns.tolist())

# %%
# [CELDA 3] Vista rápida de fuentes (sanity check)
print(tx.head(10))
print(users_raw.head(10))

# %%
# [CELDA 4] Aplanar JSON de usuarios (metadata -> columnas)
# users_raw viene como lista de objetos con:
# user_id, metadata:{name,country,email}, signup_date
users_meta = pd.json_normalize(users_raw["metadata"])
users = pd.concat(
    [users_raw.drop(columns=["metadata"]).reset_index(drop=True), users_meta.reset_index(drop=True)],
    axis=1,
)

# Renombrar a nombres consistentes
users = users.rename(
    columns={
        "name": "user_name",
        "country": "country",
        "email": "email",
    }
)

print("users flattened shape:", users.shape)
print(users.head(10))

# %%
# [CELDA 5] Tipado de usuarios (user_id, signup_date)
users["user_id"] = pd.to_numeric(users["user_id"], errors="coerce").astype("Int64")

# signup_date suele venir YYYY-MM-DD; lo parseamos seguro
users["signup_date"] = pd.to_datetime(users["signup_date"], errors="coerce")

print("dim_users user_id nulls:", int(users["user_id"].isna().sum()))
print("dim_users signup_date nulls:", int(users["signup_date"].isna().sum()))
users.info()

# %%
# [CELDA 6] Normalización básica de columnas en transacciones
# - transaction_id como string
# - user_id numérico
# - amount numérico (NaN permitido por ahora)
# - product_category como string
tx["transaction_id"] = tx["transaction_id"].astype("string").str.strip()
tx["user_id"] = pd.to_numeric(tx["user_id"], errors="coerce").astype("Int64")
tx["product_category"] = tx["product_category"].astype("string").str.strip()
tx["amount"] = pd.to_numeric(tx["amount"], errors="coerce")  # deja NaN si viene vacío/no numérico

print("tx user_id nulls:", int(tx["user_id"].isna().sum()))
print("tx amount nulls:", int(tx["amount"].isna().sum()))
tx.info()

# %%
# [CELDA 7] Validación ligera de transaction_id (no rechaza, solo perfila)
mask_txid_ok = tx["transaction_id"].astype("string").str.match(r"^T\d+$", na=False)
print("transaction_id OK:", int(mask_txid_ok.sum()), "| BAD:", int((~mask_txid_ok).sum()))

# %%
# [CELDA 8] Fechas: normalización robusta (DMY + YMD, separadores / y -)
# Aquí resolvemos lo que te pegó antes:
# - Hay YMD: 2023-06-15, 2023/05/10, etc.
# - Puede haber DMY: 01/05/2023
# - Excel confunde; nosotros NO: lo detectamos por patrón.

s = tx["date"].astype("string").fillna("").str.strip()

# recorta a 10 por si hay timestamps "YYYY-MM-DD 00:00:00"
s = s.str.slice(0, 10)

# unifica separadores a "/"
s_norm = s.str.replace("-", "/", regex=False)

mask_missing = s_norm.eq("")
mask_dmy = s_norm.str.match(r"^\d{2}/\d{2}/\d{4}$")  # 25/01/2024
mask_ymd = s_norm.str.match(r"^\d{4}/\d{2}/\d{2}$")  # 2024/01/25

tx["transaction_date"] = pd.NaT

# parse DMY explícito
tx.loc[mask_dmy, "transaction_date"] = pd.to_datetime(
    s_norm.where(mask_dmy),
    format="%d/%m/%Y",
    errors="coerce",
)

# parse YMD explícito
tx.loc[mask_ymd, "transaction_date"] = pd.to_datetime(
    s_norm.where(mask_ymd),
    format="%Y/%m/%d",
    errors="coerce",
)

# status
tx["date_status"] = "OK"
tx.loc[mask_missing, "date_status"] = "MISSING"
tx.loc[(~mask_missing) & (tx["transaction_date"].isna()), "date_status"] = "UNPARSEABLE"

# perfilado útil para ti (lo que antes era "debug")
separator_counts = {
    "contains_slash": int(s.astype(str).str.contains("/").sum()),
    "contains_dash": int(s.astype(str).str.contains("-").sum()),
}

print("date_status counts:\n", tx["date_status"].value_counts())
print("date pattern counts: DMY=", int(mask_dmy.sum()), "| YMD=", int(mask_ymd.sum()))
print("separator_counts:", separator_counts)

# si hubiera unparseables, los mostramos
if (tx["date_status"] == "UNPARSEABLE").any():
    display(tx.loc[tx["date_status"] == "UNPARSEABLE", ["transaction_id", "date"]].head(20))

# %%
# [CELDA 9] Enriquecer transacciones con usuarios (left join por user_id)
dim_users = users[["user_id", "user_name", "country", "email", "signup_date"]].copy()

df = tx.merge(
    dim_users,
    on="user_id",
    how="left",
    validate="many_to_one",  # cada transacción mapea a 1 usuario
)

print("df enriched shape:", df.shape)
print("missing user join (country null):", int(df["country"].isna().sum()))

# muestra transacciones que no encontraron usuario (si existieran)
if df["country"].isna().any():
    display(df.loc[df["country"].isna(), ["transaction_id", "user_id"]].head(20))

# %%
# [CELDA 10] Construcción de tablas finales (dim + fact)
# dim_users ya está (con tipos)
# fact_transactions: subset relevante para analytics
fact_transactions = df[
    [
        "transaction_id",
        "user_id",
        "product_category",
        "amount",
        "date",              # original
        "transaction_date",  # normalizada
        "date_status",       # OK/MISSING/UNPARSEABLE
        "country",
    ]
].copy()

print("dim_users shape:", dim_users.shape)
print("fact_transactions shape:", fact_transactions.shape)

display(dim_users.head(10))
display(fact_transactions.head(10))

# %%
# [CELDA 11] Checks rápidos (integridad básica)
print("dim_users user_id nulls:", int(dim_users["user_id"].isna().sum()))
print("fact tx missing user join (country null):", int(fact_transactions["country"].isna().sum()))
print("fact tx unparseable dates:", int((fact_transactions["date_status"] == "UNPARSEABLE").sum()))
print("fact tx missing dates:", int((fact_transactions["date_status"] == "MISSING").sum()))

# %%
# [CELDA 12] Guardar outputs (para que puedas mostrar evidencia en repo)
# - parquet/csv depende de tu preferencia. Aquí te lo dejo en CSV para que sea fácil de revisar.
dim_users_out = OUTPUT_DIR / "dim_users.csv"
fact_tx_out = OUTPUT_DIR / "fact_transactions.csv"
transform_report_out = OUTPUT_DIR / "transform_report.txt"

dim_users.to_csv(dim_users_out, index=False)
fact_transactions.to_csv(fact_tx_out, index=False)

# reporte simple de transform
lines = []
lines.append("TRANSFORM REPORT\n")
lines.append("=" * 60 + "\n")
lines.append(f"dim_users rows: {len(dim_users)}\n")
lines.append(f"fact_transactions rows: {len(fact_transactions)}\n\n")

lines.append("DATE PROFILE\n")
lines.append("-" * 60 + "\n")
lines.append(f"date_status counts: {tx['date_status'].value_counts().to_dict()}\n")
lines.append(f"separator_counts: {separator_counts}\n\n")

lines.append("NULLS\n")
lines.append("-" * 60 + "\n")
lines.append(f"tx.user_id nulls: {int(tx['user_id'].isna().sum())}\n")
lines.append(f"tx.amount nulls: {int(tx['amount'].isna().sum())}\n")
lines.append(f"fact missing user join (country null): {int(fact_transactions['country'].isna().sum())}\n")

transform_report_out.write_text("".join(lines), encoding="utf-8")

print("Saved:", dim_users_out)
print("Saved:", fact_tx_out)
print("Saved:", transform_report_out)
# %%
