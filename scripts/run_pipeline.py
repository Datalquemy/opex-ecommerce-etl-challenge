"""
Pipeline Orchestrator (ETL Runner)

Qué hace:
- Ejecuta el pipeline completo en orden:
  1) Control plane validation (DQ report)
  2) Transform (dim/fact CSVs + report)
  3) Load/Publish (DB o salida final, según tu implementación)

Cómo correr:
    python scripts/run_pipeline.py

Notas:
- Falla si faltan archivos de entrada.
- Imprime un resumen final con las rutas generadas en output/.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Entradas esperadas
TX_CSV = DATA_DIR / "transactions.csv"
USERS_JSON = DATA_DIR / "user_data.json"

# Scripts
CONTROL_PLANE = BASE_DIR / "scripts" / "control_plane_validation.py"
TRANSFORM = BASE_DIR / "scripts" / "transform.py"
LOAD = BASE_DIR / "scripts" / "load.py"  

# Salidas esperadas 
DQ_REPORT = OUTPUT_DIR / "dq_report.txt"
DIM_USERS = OUTPUT_DIR / "dim_users.csv"
FACT_TX = OUTPUT_DIR / "fact_transactions.csv"
TRANSFORM_REPORT = OUTPUT_DIR / "transform_report.txt" 


def _require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Falta {label}: {path}")


def _run_step(step_name: str, script_path: Path) -> None:
    _require_file(script_path, f"script {step_name}")
    print(f"\n[RUN] {step_name} -> {script_path.name}")
    subprocess.run([sys.executable, str(script_path)], check=True)
    print(f"[OK]  {step_name}")


def main() -> None:
    print("============================================================")
    print("ETL Runner - opex-ecommerce")
    print("BASE_DIR :", BASE_DIR)
    print("DATA_DIR :", DATA_DIR)
    print("OUTPUT   :", OUTPUT_DIR)
    print("============================================================")

    # Pre-checks
    _require_file(TX_CSV, "transactions.csv")
    _require_file(USERS_JSON, "user_data.json")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Steps
    _run_step("1) Control Plane Validation", CONTROL_PLANE)
    _run_step("2) Transform", TRANSFORM)

    # Si ya tienes load.py (DB), déjalo. Si no, comenta esta línea.
    if LOAD.exists():
        _run_step("3) Load", LOAD)
    else:
        print("\n[SKIP] 3) Load (no existe scripts/load.py)")

    # Final summary (no falla si falta algo; solo informa)
    print("\n==================== OUTPUT SUMMARY ====================")
    for p in [DQ_REPORT, TRANSFORM_REPORT, DIM_USERS, FACT_TX]:
        status = "OK" if p.exists() else "MISSING"
        print(f"- {status:<7} {p}")
    print("========================================================")
    print("DONE")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nFAILED:", e)
        sys.exit(1)