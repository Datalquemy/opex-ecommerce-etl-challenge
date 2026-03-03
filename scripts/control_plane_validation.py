"""
Control Plane - Data Quality Checks (transactions.csv)

Qué hace:
- Carga data/transactions.csv
- Ejecuta validaciones (NO limpia datos):
  1) transaction_id formato ^T\\d+$
  2) user_id numérico (reporta inválidos con transaction_id)
  3) product_category perfilado (categorías observadas + nulos/vacíos)
  4) amount numérico (reporta inválidos con transaction_id)
  5) date formato (acepta DMY o YMD con / o -; reporta inválidos con transaction_id)
- Escribe/actualiza un reporte en output/dq_report.txt

Cómo correr:
    python scripts/validate_transactions.py

Nota:
- Este script es "control plane": solo reporta anomalías.
- La limpieza/normalización se hará después en transform.py.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd


# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_PATH = OUTPUT_DIR / "dq_report.txt"


# ---------- Helpers ----------
def load_transactions() -> pd.DataFrame:
    """Load transactions.csv"""
    path = DATA_DIR / "transactions.csv"
    df = pd.read_csv(path)
    print("CSV cargado ✅")
    print("Filas:", len(df))
    print("Columnas:", list(df.columns))
    return df


def append_report_section(title: str, lines: list[str]) -> None:
    """Append a section to dq_report.txt (creates file if missing)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    existing = ""
    if REPORT_PATH.exists():
        existing = REPORT_PATH.read_text(encoding="utf-8")

    section = []
    section.append("\n" + "=" * 60 + "\n")
    section.append(f"CHECK: {title}\n")
    section.extend(lines)

    REPORT_PATH.write_text(existing + "".join(section), encoding="utf-8")


def format_summary_line(**kwargs) -> str:
    """Builds a single summary line like Total(x), Approved(y), Rejected(z)."""
    parts = []
    for k, v in kwargs.items():
        parts.append(f"{k}({v})")
    return ", ".join(parts) + "\n"


def write_invalid_rows(label: str, invalid_rows: list[dict]) -> list[str]:
    """Format invalid rows for report."""
    if not invalid_rows:
        return []
    out = [f"{label}:\n"]
    for row in invalid_rows:
        # Esperamos keys como transaction_id + valor de la columna
        tid = row.get("transaction_id", "")
        # imprimir el resto de campos
        rest = {k: v for k, v in row.items() if k != "transaction_id"}
        out.append(f" - {tid}: {rest}\n")
    return out
def load_users() -> pd.DataFrame:
    path = DATA_DIR / "user_data.json"
    df = pd.read_json(path)
    print("JSON users cargado ✅")
    print("Filas:", len(df))
    return df


# ---------- Validations (control plane) ----------
def validate_transaction_id(df: pd.DataFrame) -> dict:
    total = len(df)
    mask_valid = df["transaction_id"].astype(str).str.match(r"^T\d+$", na=False)
    approved = int(mask_valid.sum())
    rejected = int(total - approved)

    print("=== Validación transaction_id ===")
    print(f"Total: {total} | Aprobados: {approved} | Rechazados: {rejected}")

    invalid_rows = []
    if rejected > 0:
        invalid_rows = df.loc[~mask_valid, ["transaction_id"]].to_dict(orient="records")
        print("\nEjemplos inválidos (top 10):")
        print(df.loc[~mask_valid, ["transaction_id"]].head(10))
    else:
        print("\n✅ Todos los transaction_id cumplen el formato ^T\\d+$")

    return {
        "check": "transaction_id_format",
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "invalid_rows": invalid_rows,
    }


def validate_user_id_numeric(df: pd.DataFrame) -> dict:
    total = len(df)
    user_num = pd.to_numeric(df["user_id"], errors="coerce")
    mask_invalid = user_num.isna()

    rejected = int(mask_invalid.sum())
    approved = int(total - rejected)
    total_distinct = int(df["user_id"].nunique(dropna=True))

    invalid_rows = df.loc[mask_invalid, ["transaction_id", "user_id"]].to_dict(orient="records")

    print("=== Validación user_id (numérico) ===")
    print(f"Total filas: {total}")
    print(f"User TotalDistinct: {total_distinct}")
    print(f"Aprobados: {approved} | Rechazados: {rejected}")

    if rejected > 0:
        print("\nInválidos (transaction_id, user_id) - top 10:")
        print(df.loc[mask_invalid, ["transaction_id", "user_id"]].head(10))
    else:
        print("\n✅ Todos los user_id son numéricos")

    return {
        "check": "user_id_numeric",
        "total": total,
        "total_distinct": total_distinct,
        "approved": approved,
        "rejected": rejected,
        "invalid_rows": invalid_rows,
    }
def validate_user_fk(df_tx: pd.DataFrame, df_users: pd.DataFrame) -> dict:
    total = len(df_tx)

    users_set = set(df_users["user_id"])

    mask_invalid = ~df_tx["user_id"].isin(users_set)

    rejected = int(mask_invalid.sum())
    approved = total - rejected

    invalid_rows = df_tx.loc[mask_invalid, ["transaction_id", "user_id"]].to_dict(orient="records")

    print("=== Validación FK user_id ∈ users.json ===")
    print(f"Total: {total}")
    print(f"Aprobados: {approved} | Rechazados: {rejected}")

    if rejected > 0:
        print("\nInválidos (transaction_id, user_id) - top 10:")
        print(df_tx.loc[mask_invalid, ["transaction_id", "user_id"]].head(10))
    else:
        print("\n✅ Todos los user_id existen en users.json")

    return {
        "check": "user_fk_exists",
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "invalid_rows": invalid_rows,
    }

def validate_product_category_profile(df: pd.DataFrame) -> dict:
    total = len(df)
    cat_raw = df["product_category"]
    cat_str = cat_raw.astype(str)

    mask_null = cat_raw.isna()
    mask_empty = cat_str.str.strip().eq("")
    rejected = int((mask_null | mask_empty).sum())
    approved = int(total - rejected)

    counts = (
        df["product_category"]
        .fillna("<<NULL>>")
        .astype(str)
        .str.strip()
        .replace("", "<<EMPTY>>")
        .value_counts()
        .to_dict()
    )
    observed = sorted([k for k in counts.keys() if k not in ["<<NULL>>", "<<EMPTY>>"]])

    print("=== Validación product_category (perfilado) ===")
    print(f"Total: {total} | Aprobados: {approved} | Rechazados(null/empty): {rejected}")
    print("Categorías observadas:", observed)

    return {
        "check": "product_category_profile",
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "observed_categories": observed,
        "counts": counts,
    }


def validate_amount_numeric(df: pd.DataFrame) -> dict:
    total = len(df)
    amount_num = pd.to_numeric(df["amount"], errors="coerce")
    mask_invalid = amount_num.isna()

    rejected = int(mask_invalid.sum())
    approved = int(total - rejected)

    invalid_rows = df.loc[mask_invalid, ["transaction_id", "amount"]].to_dict(orient="records")

    print("=== Validación amount ===")
    print(f"Total: {total}")
    print(f"Aprobados: {approved} | Rechazados: {rejected}")

    if rejected > 0:
        print("\nInválidos (transaction_id, amount) - top 10:")
        print(df.loc[mask_invalid, ["transaction_id", "amount"]].head(10))
    else:
        print("\n✅ Todos los amount son numéricos y no nulos")

    return {
        "check": "amount_numeric",
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "invalid_rows": invalid_rows,
    }


def validate_date_format(df: pd.DataFrame) -> dict:
    total = len(df)
    s = df["date"].astype(str).str.strip()

    mask_null = df["date"].isna()
    mask_empty = s.eq("")

    # DD/MM/YYYY o DD-MM-YYYY
    pattern_dmy = r"^\d{2}[/-]\d{2}[/-]\d{4}$"
    # YYYY/MM/DD o YYYY-MM-DD
    pattern_ymd = r"^\d{4}[/-]\d{2}[/-]\d{2}$"

    mask_ok = s.str.match(pattern_dmy) | s.str.match(pattern_ymd)
    mask_invalid = (~mask_null) & (~mask_empty) & (~mask_ok)

    rejected_null_empty = int((mask_null | mask_empty).sum())
    rejected_bad_format = int(mask_invalid.sum())
    rejected = rejected_null_empty + rejected_bad_format
    approved = int(total - rejected)

    invalid_rows = df.loc[(mask_null | mask_empty | mask_invalid), ["transaction_id", "date"]].to_dict(orient="records")

    print("=== Validación date (formato) ===")
    print(f"Total: {total}")
    print(f"Aprobados: {approved}")
    print(f"Rechazados total: {rejected}")
    print(f" - Null/Empty: {rejected_null_empty}")
    print(f" - Bad format: {rejected_bad_format}")

    if rejected > 0:
        print("\nInválidos (transaction_id, date) - top 10:")
        print(df.loc[(mask_null | mask_empty | mask_invalid), ["transaction_id", "date"]].head(10))
    else:
        print("\n✅ Todas las fechas cumplen formato (DMY o YMD)")

    return {
        "check": "date_format",
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "rejected_null_empty": rejected_null_empty,
        "rejected_bad_format": rejected_bad_format,
        "invalid_rows": invalid_rows,
    }
def validate_users_json(df: pd.DataFrame) -> dict:
    total = len(df)

    # user_id numeric
    user_num = pd.to_numeric(df["user_id"], errors="coerce")
    mask_invalid_user_id = user_num.isna()

    # duplicates
    mask_duplicate = df.duplicated(subset=["user_id"], keep=False)

    # metadata existence
    mask_metadata_null = df["metadata"].isna()

    # Expand metadata safely
    metadata_df = pd.json_normalize(df["metadata"])

    required_meta_fields = ["name", "country", "email"]
    missing_meta_fields = [
        col for col in required_meta_fields if col not in metadata_df.columns
    ]

    # email simple regex
    email_pattern = r"^[^@]+@[^@]+\.[^@]+$"
    mask_bad_email = ~metadata_df["email"].astype(str).str.match(email_pattern)

    # signup_date parse
    signup_parsed = pd.to_datetime(df["signup_date"], errors="coerce")
    mask_bad_signup = signup_parsed.isna()

    rejected = (
        mask_invalid_user_id
        | mask_duplicate
        | mask_metadata_null
        | mask_bad_email
        | mask_bad_signup
    )

    rejected_count = int(rejected.sum())
    approved = total - rejected_count

    invalid_rows = df.loc[rejected, ["user_id", "signup_date"]].to_dict(orient="records")

    print("=== Validación users.json ===")
    print(f"Total: {total}")
    print(f"Aprobados: {approved} | Rechazados: {rejected_count}")

    if missing_meta_fields:
        print("⚠ Faltan campos metadata:", missing_meta_fields)

    if rejected_count > 0:
        print("\nUsuarios inválidos (top 10):")
        print(df.loc[rejected].head(10))
    else:
        print("\n✅ users.json válido")

    return {
        "check": "users_json_validation",
        "total": total,
        "approved": approved,
        "rejected": rejected_count,
        "invalid_rows": invalid_rows,
    }

# ---------- Main ----------
def main() -> None:
    # Reset report on each execution
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("", encoding="utf-8")
    print("Reporte reiniciado ✅")

    tx = load_transactions() 
    users = load_users()
   
    results = []
    results.append(validate_transaction_id(tx))
    results.append(validate_user_id_numeric(tx))
    results.append(validate_product_category_profile(tx))
    results.append(validate_amount_numeric(tx))
    results.append(validate_date_format(tx))
    results.append(validate_user_fk(tx, users))
    results.append(validate_users_json(users))

    # Write report sections
    for r in results:
        check = r["check"]

        # Summary line varies by check
        if check == "user_id_numeric":
            summary = format_summary_line(
                Total=r["total"],
                TotalDistinct=r["total_distinct"],
                Aprobados=r["approved"],
                Rechazados=r["rejected"],
            )
            lines = [summary]
            lines += write_invalid_rows("Invalid rows (transaction_id, user_id)", r["invalid_rows"])
            append_report_section(check, lines)

        elif check in ("transaction_id_format", "amount_numeric"):
            summary = format_summary_line(
                Total=r["total"],
                Aprobados=r["approved"],
                Rechazados=r["rejected"],
            )
            lines = [summary]
            label = "Invalid rows (transaction_id)"
            if check == "amount_numeric":
                label = "Invalid rows (transaction_id, amount)"
            lines += write_invalid_rows(label, r["invalid_rows"])
            append_report_section(check, lines)

        elif check == "product_category_profile":
            summary = format_summary_line(
                Total=r["total"],
                Aprobados=r["approved"],
                Rechazados=r["rejected"],
            )
            lines = [summary, "Observed categories:\n"]
            for c in r["observed_categories"]:
                lines.append(f" - {c}\n")
            lines.append("Counts:\n")
            for k, v in r["counts"].items():
                lines.append(f" - {k}: {v}\n")
            append_report_section(check, lines)

        elif check == "date_format":
            summary = (
                f"Total({r['total']}), Aprobados({r['approved']}), Rechazados({r['rejected']}) | "
                f"Null/Empty({r['rejected_null_empty']}), BadFormat({r['rejected_bad_format']})\n"
            )
            lines = [summary]
            lines += write_invalid_rows("Invalid rows (transaction_id, date)", r["invalid_rows"])
            append_report_section(check, lines)
        elif check == "user_fk_exists":
            summary = format_summary_line(
                Total=r["total"],
                Aprobados=r["approved"],
                Rechazados=r["rejected"],
            )
            lines = [summary]
            lines += write_invalid_rows("Invalid rows (transaction_id, user_id)", r["invalid_rows"])
            append_report_section(check, lines)
        elif check == "users_json_validation":
            summary = format_summary_line(
            Total=r["total"],
            Aprobados=r["approved"],
            Rechazados=r["rejected"],
            )
            lines = [summary]
            lines += write_invalid_rows("Invalid users", r["invalid_rows"])
            append_report_section(check, lines)     
    print("\nReporte actualizado ✅:", REPORT_PATH)


if __name__ == "__main__":
    main()