"""
Transformations for Medallion Architecture Pipeline:
Bronze → Silver → Quality.

Todas las funciones reciben ds_nodash para operar por fecha.
Cada función devuelve rutas de archivos como strings, aptas para XCom.
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path


# ============================================================
# BRONZE LAYER
# ============================================================

def extract_bronze(ds_nodash: str, raw_dir: Path) -> str:
    """
    Bronze: lee el archivo RAW del día desde raw_dir.
    Ejemplo de archivo esperado: transactions_20251206.csv
    No transforma, solo valida existencia y devuelve la ruta.
    """
    input_path = Path(raw_dir) / f"transactions_{ds_nodash}.csv"

    if not input_path.exists():
        raise FileNotFoundError(f"[Bronze] No se encontró archivo RAW: {input_path}")

    print(f"[Bronze] Archivo RAW detectado: {input_path}")
    return str(input_path)


# ============================================================
# SILVER LAYER
# ============================================================

def clean_daily_transactions(ds_nodash: str, raw_dir: Path, clean_dir: Path) -> str:
    """
    Silver: limpieza básica del archivo RAW.
    - Lee CSV del día
    - Elimina filas vacías
    - Normaliza columnas clave
    - Genera parquet limpio
    """
    input_path = Path(raw_dir) / f"transactions_{ds_nodash}.csv"
    output_path = Path(clean_dir) / f"transactions_{ds_nodash}_clean.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"[Silver] No existe archivo RAW para limpiar: {input_path}")

    df = pd.read_csv(input_path)

    # Conversión mínima para permitir filtrar valores inválidos
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Filtro mínimo: eliminar filas donde amount quedó nulo
    df = df[df["amount"].notna()]

    # Limpieza mínima para el examen
    df = df.dropna(how="all")

    # Normalizaciones típicas
    if "status" in df.columns:
        df["status"] = (
            df["status"]
            .astype(str)
            .str.lower()
            .str.strip()
        )

    # Guardar limpio
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"[Silver] Archivo limpio generado: {output_path}")
    return str(output_path)


# ============================================================
# QUALITY LAYER
# ============================================================

def run_quality_checks(ds_nodash: str, clean_dir: Path, quality_dir: Path) -> str:
    """
    Quality: validaciones mínimas para el examen.
    - Verifica columnas obligatorias
    - Verifica que no haya amounts negativos
    - Escribe un reporte simple
    """
    clean_path = Path(clean_dir) / f"transactions_{ds_nodash}_clean.parquet"
    report_path = Path(quality_dir) / f"quality_report_{ds_nodash}.txt"

    if not clean_path.exists():
        raise FileNotFoundError(f"[Quality] No existe archivo Silver: {clean_path}")

    df = pd.read_parquet(clean_path)

    issues = []

    required_columns = ["transaction_id", "amount", "status"]
    for col in required_columns:
        if col not in df.columns:
            issues.append(f"Missing required column: {col}")

    if "amount" in df.columns:
        if (df["amount"] < 0).any():
            issues.append("Negative amounts detected")

    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w") as f:
        if issues:
            f.write("QUALITY CHECK FAILED\n")
            f.write("\n".join(issues))
        else:
            f.write("QUALITY CHECK PASSED\n")

    print(f"[Quality] Reporte generado en: {report_path}")
    return str(report_path)
