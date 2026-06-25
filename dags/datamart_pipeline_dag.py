"""
DAG principal del pipeline DataMart S.A.S.

Flujo: extracción (paralela) -> transformación -> validación de calidad
-> carga al Data Warehouse.

- schedule diario (todos los días a la medianoche).
- reintentos automáticos ante fallos transitorios.
- idempotente: ver natural_key + UPSERT en load.py.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scripts.extract import extraer_ventas_diarias, extraer_historico
from scripts.transform import transformar_y_validar
from scripts.quality_checks import validar_calidad
from scripts.load import cargar_datawarehouse

default_args = {
    "owner": "miguel",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="datamart_pipeline",
    description="Pipeline ETL DataMart: ventas diarias + histórico -> Data Warehouse",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["datamart", "etl"],
) as dag:

    extraer_ventas = PythonOperator(
        task_id="extraer_ventas_diarias",
        python_callable=extraer_ventas_diarias,
    )

    extraer_hist = PythonOperator(
        task_id="extraer_historico",
        python_callable=extraer_historico,
    )

    transformar = PythonOperator(
        task_id="transformar_y_validar",
        python_callable=transformar_y_validar,
    )

    validar_calidad_task = PythonOperator(
        task_id="validar_calidad",
        python_callable=validar_calidad,
    )

    cargar = PythonOperator(
        task_id="cargar_datawarehouse",
        python_callable=cargar_datawarehouse,
    )

    [extraer_ventas, extraer_hist] >> transformar >> validar_calidad_task >> cargar