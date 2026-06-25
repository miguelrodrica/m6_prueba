"""
Carga idempotente al Data Warehouse.

Decisión de diseño: usa la Airflow Connection 'dw_postgres_conn' a través
de BaseHook (no PostgresHook), para no depender del provider
apache-airflow-providers-postgres -- BaseHook ya viene en el core de
Airflow. La conexión real a Postgres se hace con psycopg2.

Idempotencia:
  - dim_producto / dim_cliente: UPSERT por su clave natural (stock_code /
    customer_id). Correr el DAG dos veces actualiza, no duplica.
  - fact_transacciones: UPSERT por natural_key (ver transform.py).
  - log_rechazos: NO tiene clave natural única (es un log). Para que
    re-ejecutar el mismo día no duplique el log, se borran los rechazos
    de las fuentes de este run antes de reinsertar el snapshot actual.
    Esto es una limitación documentada: si quisieras conservar historial
    de rechazos entre corridas distintas, necesitarías partición por
    fecha de ejecución, no solo por fuente.
"""

import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from airflow.hooks.base import BaseHook

ESQUEMA = "datamart"


def _conectar_dw():
    conn_info = BaseHook.get_connection("dw_postgres_conn")
    return psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )


def _upsert_dim_producto(cur, df):
    if df.empty:
        return
    registros = list(df[["stock_code", "nombre_canonico", "categoria", "activo"]]
                      .itertuples(index=False, name=None))
    execute_values(cur, f"""
        INSERT INTO {ESQUEMA}.dim_producto (stock_code, nombre, categoria, activo)
        VALUES %s
        ON CONFLICT (stock_code) DO UPDATE SET
            nombre = EXCLUDED.nombre,
            categoria = EXCLUDED.categoria,
            activo = EXCLUDED.activo,
            fecha_actualizacion = NOW()
    """, registros)


def _upsert_dim_cliente(cur, df):
    if df.empty:
        return
    registros = list(df[["customer_id", "pais", "es_invitado"]]
                      .itertuples(index=False, name=None))
    execute_values(cur, f"""
        INSERT INTO {ESQUEMA}.dim_cliente (customer_id, pais, es_invitado)
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            pais = EXCLUDED.pais,
            es_invitado = EXCLUDED.es_invitado,
            fecha_actualizacion = NOW()
    """, registros)


def _mapear_producto_id(cur):
    cur.execute(f"SELECT stock_code, producto_id FROM {ESQUEMA}.dim_producto")
    return dict(cur.fetchall())


def _mapear_cliente_id(cur):
    cur.execute(f"SELECT customer_id, cliente_id FROM {ESQUEMA}.dim_cliente")
    return dict(cur.fetchall())


def _upsert_fact(cur, df):
    """Devuelve (insertados, actualizados) usando el truco de Postgres
    xmax = 0 para distinguir si la fila era nueva o ya existía."""
    if df.empty:
        return 0, 0

    columnas = [
        "natural_key", "invoice_no", "producto_id", "cliente_id",
        "fecha_utc", "fecha_dia", "pais", "cantidad", "precio_unitario",
        "revenue_bruto", "revenue_devolucion", "revenue_neto",
        "tipo_linea", "fuente",
    ]
    registros = list(df[columnas].itertuples(index=False, name=None))

    resultados = execute_values(cur, f"""
        INSERT INTO {ESQUEMA}.fact_transacciones (
            natural_key, invoice_no, producto_id, cliente_id,
            fecha_utc, fecha_dia, pais, cantidad, precio_unitario,
            revenue_bruto, revenue_devolucion, revenue_neto,
            tipo_linea, fuente
        )
        VALUES %s
        ON CONFLICT (natural_key) DO UPDATE SET
            cantidad = EXCLUDED.cantidad,
            precio_unitario = EXCLUDED.precio_unitario,
            revenue_bruto = EXCLUDED.revenue_bruto,
            revenue_devolucion = EXCLUDED.revenue_devolucion,
            revenue_neto = EXCLUDED.revenue_neto,
            tipo_linea = EXCLUDED.tipo_linea
        RETURNING (xmax = 0) AS fue_insertado
    """, registros, fetch=True)

    insertados = sum(1 for fila in resultados if fila[0])
    actualizados = len(resultados) - insertados
    return insertados, actualizados


def _reemplazar_rechazos(cur, df):
    fuentes_de_este_run = df["fuente"].unique().tolist()
    if fuentes_de_este_run:
        cur.execute(
            f"DELETE FROM {ESQUEMA}.log_rechazos WHERE fuente = ANY(%s)",
            (fuentes_de_este_run,),
        )

    if df.empty:
        return 0

    registros = list(df[["fuente", "invoice_no", "stock_code",
                          "motivo", "payload_original"]].itertuples(
        index=False, name=None
    ))
    execute_values(cur, f"""
        INSERT INTO {ESQUEMA}.log_rechazos
            (fuente, invoice_no, stock_code, motivo, payload_original)
        VALUES %s
    """, [(f, inv, sc, mot, json.loads(payload) if isinstance(payload, str) else payload)
          for f, inv, sc, mot, payload in registros])
    return len(registros)


def _insertar_auditoria(cur, metricas):
    cur.execute(f"""
        INSERT INTO {ESQUEMA}.etl_auditoria
            (proceso, fuente, filas_leidas, filas_validas,
             filas_insertadas, filas_actualizadas, filas_rechazadas,
             fecha_fin, estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
    """, (
        "pipeline_datamart",
        "transacciones_diarias+historico",
        metricas["filas_leidas"],
        metricas["filas_validas"],
        metricas["filas_insertadas"],
        metricas["filas_actualizadas"],
        metricas["filas_rechazadas"],
        "OK",
    ))


def cargar_datawarehouse(**context):
    ti = context["ti"]
    info = ti.xcom_pull(task_ids="transformar_y_validar")

    dim_producto = pd.read_parquet(info["ruta_dim_producto"])
    dim_cliente = pd.read_parquet(info["ruta_dim_cliente"])
    fact = pd.read_parquet(info["ruta_fact"])
    rechazos = pd.read_parquet(info["ruta_rechazos"])

    conn = _conectar_dw()
    cur = conn.cursor()
    try:
        _upsert_dim_producto(cur, dim_producto)
        _upsert_dim_cliente(cur, dim_cliente)
        conn.commit()

        mapa_producto = _mapear_producto_id(cur)
        mapa_cliente = _mapear_cliente_id(cur)

        fact["producto_id"] = fact["stock_code"].map(mapa_producto)
        fact["cliente_id"] = fact["customer_id"].map(mapa_cliente)

        sin_match = fact["producto_id"].isna() | fact["cliente_id"].isna()
        if sin_match.any():
            # Red de seguridad: no debería pasar si las dimensiones se
            # cargaron correctamente justo antes. Si pasa, se documenta
            # como filas perdidas en vez de fallar todo el pipeline.
            fact = fact[~sin_match]

        insertados, actualizados = _upsert_fact(cur, fact)
        filas_rechazadas = _reemplazar_rechazos(cur, rechazos)

        _insertar_auditoria(cur, {
            "filas_leidas": info["filas_leidas"],
            "filas_validas": info["filas_validas"],
            "filas_insertadas": insertados,
            "filas_actualizadas": actualizados,
            "filas_rechazadas": filas_rechazadas,
        })

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return {
        "filas_insertadas": insertados,
        "filas_actualizadas": actualizados,
        "filas_rechazadas": filas_rechazadas,
    }