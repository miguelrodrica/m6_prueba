"""
Validación de calidad: corre DESPUÉS de transform.py y ANTES de load.py.

Decisión de diseño: esta etapa no vuelve a limpiar datos (eso ya lo hizo
transform.py) -- su trabajo es verificar que el resultado de la
transformación cumple las reglas mínimas de negocio antes de tocar el
Data Warehouse. Si algo falla aquí, el pipeline se detiene ANTES de cargar
nada corrupto (mejor fallar ruidosamente que cargar datos malos en
silencio).
"""

import pandas as pd


class ErrorCalidadDatos(Exception):
    """Se lanza cuando los datos transformados no pasan una validación
    mínima. Detiene el DAG antes de llegar a cargar.py."""


def validar_calidad(**context):
    ti = context["ti"]
    info = ti.xcom_pull(task_ids="transformar_y_validar")

    fact = pd.read_parquet(info["ruta_fact"])
    rechazos = pd.read_parquet(info["ruta_rechazos"])

    errores = []

    # 1. No debe haber ventas con precio <= 0 (transform.py debió
    #    haberlas movido a rechazos).
    ventas = fact[fact["tipo_linea"] == "VENTA"]
    ventas_precio_invalido = (ventas["precio_unitario"] <= 0).sum()
    if ventas_precio_invalido > 0:
        errores.append(
            f"{ventas_precio_invalido} ventas con precio_unitario <= 0 "
            "llegaron hasta fact_transacciones (deberían estar en rechazos)."
        )

    # 2. natural_key no debe tener duplicados (rompería el UPSERT y la
    #    idempotencia).
    duplicados_nk = fact["natural_key"].duplicated().sum()
    if duplicados_nk > 0:
        errores.append(
            f"{duplicados_nk} natural_key duplicadas en fact_transacciones."
        )

    # 3. fecha_utc no debe tener nulos (rompería los reportes mensuales).
    fechas_nulas = fact["fecha_utc"].isna().sum()
    if fechas_nulas > 0:
        errores.append(f"{fechas_nulas} filas con fecha_utc nula.")

    # 4. producto y cliente no deben venir vacíos.
    if fact["stock_code"].isna().any() or (fact["stock_code"] == "").any():
        errores.append("Hay filas con stock_code vacío en fact_transacciones.")

    # 5. Métrica informativa (no bloquea el pipeline): tasa de rechazo.
    total_procesado = len(fact) + len(rechazos)
    tasa_rechazo = len(rechazos) / total_procesado if total_procesado else 0
    if tasa_rechazo > 0.10:
        # Más del 10% de rechazos es sospechoso -- no detiene el DAG,
        # pero queda en logs para que alguien lo revise.
        print(
            f"ADVERTENCIA: tasa de rechazo de {tasa_rechazo:.1%}, "
            "por encima del umbral esperado del 10%."
        )

    if errores:
        raise ErrorCalidadDatos(
            "Validación de calidad falló:\n- " + "\n- ".join(errores)
        )

    return {
        "filas_validadas": len(fact),
        "tasa_rechazo": round(tasa_rechazo, 4),
        "estado": "OK",
    }