"""Airflow DAG for Medallion Architecture Pipeline: Bronze → Silver → Quality → Gold → Tests"""

from __future__ import annotations

import os
import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================
# Variables de entorno del proyecto
# ============================================================

from pathlib import Path

PROJECT_DIR = Path("/home/rantez/MIA/IngenieriaSoftware/Datos/00_examen/examen_ing_de_sw_n_data_final")

# --- Directorios principales ---
DBT_DIR = PROJECT_DIR / "dbt"
DATA_DIR = PROJECT_DIR / "data"
DBT_PROFILES_DIR = PROJECT_DIR / "profiles"
DUCKDB_PATH = PROJECT_DIR / "warehouse"

RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
QUALITY_DIR = DATA_DIR / "quality"

# ============================================================
# Ajuste del PATH para importar desde include/
# ============================================================

if str(PROJECT_DIR) not in sys.path:
    sys.path.append(str(PROJECT_DIR))

from include.transformations import (
    extract_bronze,
    clean_daily_transactions,
    run_quality_checks,
)

# ============================================================
# Funciones wrapper para ejecutar dbt
# ============================================================

def run_dbt_models(ds_nodash: str, **context):
    """
    Ejecuta dbt run pasando ds_nodash y clean_dir como variables.
    """
    vars_payload = {
        "ds": ds_nodash,
        "clean_dir": str(CLEAN_DIR),
    }

    cmd = [
        "dbt",
        "run",
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--vars", json.dumps(vars_payload),
    ]

    subprocess.run(cmd, cwd=DBT_DIR, check=True)
    print("[Gold] dbt run ejecutado correctamente.")


def run_dbt_tests(ds_nodash: str, **context):
    """
    Ejecuta dbt test pasando ds_nodash y clean_dir como variables.
    """
    vars_payload = {
        "ds": ds_nodash,
        "clean_dir": str(CLEAN_DIR),
    }

    cmd = [
        "dbt",
        "test",
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--vars", json.dumps(vars_payload),
    ]

    subprocess.run(cmd, cwd=DBT_DIR, check=True)
    print("[Tests] dbt test ejecutado correctamente.")


# ============================================================
# Definición del DAG
# ============================================================

with DAG(
    dag_id="medallion_pipeline",
    description="Bronze/Silver/Gold pipeline with Pandas + DuckDB + dbt",
    schedule="0 6 * * *",  # diario a las 06:00 UTC
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
) as medallion_dag:

    # ------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------
    bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_bronze,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "raw_dir": RAW_DIR,
        },
    )

    # ------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------
    silver = PythonOperator(
        task_id="clean_silver",
        python_callable=clean_daily_transactions,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "raw_dir": RAW_DIR,
            "clean_dir": CLEAN_DIR,
        },
    )

    # ------------------------------------------------------------
    # QUALITY LAYER
    # ------------------------------------------------------------
    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "clean_dir": CLEAN_DIR,
            "quality_dir": QUALITY_DIR,
        },
    )

    # ------------------------------------------------------------
    # GOLD → dbt run
    # ------------------------------------------------------------
    dbt_run = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )

    # ------------------------------------------------------------
    # TESTS → dbt test
    # ------------------------------------------------------------
    dbt_test = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=run_dbt_tests,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )

    # ------------------------------------------------------------
    # Flujo final
    # ------------------------------------------------------------
    bronze >> silver >> quality >> dbt_run >> dbt_test
