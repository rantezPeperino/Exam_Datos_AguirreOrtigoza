# Medallion 

## 1. Introducción
Este proyecto implementa un pipeline batch diario basado en la arquitectura Medallion (Bronze → Silver → Quality → Gold), orquestado con Apache Airflow y modelado con dbt. Los datos provienen de archivos CSV diarios con transacciones. El objetivo final es construir un data mart confiable, auditable y reproducible.

## 2. Flujo General del Pipeline
RAW (CSV)
   ↓ Bronze
Silver (Parquet limpio)
   ↓ Quality
Reporte de validaciones
   ↓ Gold (dbt run)
Tablas analíticas
   ↓ Tests (dbt test)

El pipeline es reproducible por fecha mediante ds_nodash.

## 3. Capas del Pipeline

### 3.1 Bronze — extract_bronze
Responsabilidades:
- Validar existencia del CSV del día.
- No transformar los datos.
- Devolver la ruta del archivo RAW.

Beneficios:
- Trazabilidad total.
- Permite reprocesos históricos.

### 3.2 Silver — clean_daily_transactions
Responsabilidades:
- Leer archivo RAW.
- Convertir amount a numérico.
- Normalizar status.
- Eliminar filas inválidas (amount nulo). Esta decision se toma desde el negocio. Nosotros optamos por elminar, se puede transformar el null en 0.0, pero hubo un fallo, se elimna
  no tenemos reportes de fallos. Se tendria que armar otro sql, no es parte del ejercicio.
- Guardar la salida limpia como Parquet.

Beneficios:
- Reduce errores aguas abajo.
- Garantiza datos consistentemente tipados.
- Prepara el dataset para dbt.

### 3.3 Quality Layer — run_quality_checks
Validaciones aplicadas:
- Columnas requeridas: transaction_id, amount, status.
- Detecta montos negativos.
- Escribe un reporte en data/quality.

Beneficios:
- Asegura integridad previa al modelado.
- Facilita auditoría y debugging.

### 3.4 Gold Layer — dbt run
dbt genera dos modelos:

1) stg_transactions
- Casteo de tipos.
- Normalización de status.
- Derivación de transaction_date.
- Filtro de estados válidos.
- Eliminación de registros inconsistentes.

2) fct_customer_transactions
- Cantidad de transacciones por cliente.
- Total de montos completados.
- Total de montos generales.

Beneficios:
- Semántica analítica clara.
- Cálculos estandarizados de negocio.
- Base sólida para BI, reporting y ML.

## 4. Tests en dbt

### Tests en Staging (stg_transactions)
- not_null en campos principales.
- unique(transaction_id) para evitar duplicados.
- accepted_values(status) restringe dominio.
- non_negative(amount) asegura montos válidos.

### Tests en Gold (fct_customer_transactions)
- not_null en métricas agregadas.
- non_negative en totales y conteos.

Justificación:
- Las métricas no pueden ser negativas ni nulas.
- Garantizan integridad analítica.

## 5. Orquestación con Airflow
El DAG medallion_pipeline ejecuta en orden:

1. extract_bronze
2. clean_silver
3. quality_checks
4. run_dbt_models
5. run_dbt_tests

Características:
- Dependencias explícitas.
- Reintentos automáticos.
- Ejecución diaria.
- Reprocesos manuales por fecha usando ds_nodash.

## 6. Estructura del Proyecto
dags/
  medallion_medallion_dag.py  
include/
  transformations.py  
dbt/
  models/staging  
  models/marts  
  tests  
  dbt_project.yml  
profiles/
  profiles.yml  
data/
  raw/  
  clean/  
  quality/  
warehouse/

## 7. Operación

Reprocesar un día manualmente:
Se ejecuta el DAG indicando {"ds": "YYYY-MM-DD"}.

Errores frecuentes:
- RAW faltante: revisar data/raw.
- Tests fallados por montos negativos: revisar Silver.
- accepted_values fallando: status no normalizado.
- Error en perfil de DuckDB: revisar profiles.yml.

## 8. Extensibilidad
El sistema permite:
- Agregar columnas sin romper modelos.
- Nuevos data marts.
- Nuevas fuentes RAW.
- Reglas de calidad adicionales.
- Escalar hacia modelos lakehouse.

## 9. Mejoras posibles
Creemos que el proyecto es bastante robusto y escalable en su estado actual, aún así proponemos las siguientes potenciales mejoras: 
- Almacener archivos en algún servicio de storage en la nube
- Hostear el DAG de Airflow en algún servicio de cómputo en la nube
- Paralelizar la ejecución de días pendientes cuando se acumulen transacciones históricas
- Aprovechar la posibilidad que da Airflow para definir entornos "dev" y "prod" para poder probar cambios (en dev) sin afectar la data productiva (en prod)

## 10. Conclusión
El proyecto aplica buenas prácticas modernas de ingeniería de datos:
- Arquitectura Medallion.
- Orquestación con Airflow.
- Limpieza reproducible con Pandas.
- Modelado declarativo con dbt.
- Testing formal automatizado.
- Almacenamiento eficiente con DuckDB.

Es una base robusta para analítica, BI y machine learning.
