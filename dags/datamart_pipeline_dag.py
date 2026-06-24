from airflow.providers.standard.operators.python import PythonOperator
from scripts.extract import extraer_ventas_diarias, extraer_historico
from scripts.transform import transformar_y_validar
from scripts.load import cargar_datawarehouse

extraer_1 = PythonOperator(
    task_id="extraer_ventas_diarias",
    python_callable=extraer_ventas_diarias,
)
extraer_2 = PythonOperator(
    task_id="extraer_historico",
    python_callable=extraer_historico,
)
transformar = PythonOperator(
    task_id="transformar_y_validar",
    python_callable=transformar_y_validar,
)
cargar = PythonOperator(
    task_id="cargar_datawarehouse",
    python_callable=cargar_datawarehouse,
)

[extraer_1, extraer_2] >> transformar >> cargar