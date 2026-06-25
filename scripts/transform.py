"""
Transformación y reglas de negocio del pipeline DataMart.

Aplica, en orden, las decisiones documentadas en docs/decisiones_tecnicas.md:
  1. Normaliza código de producto (mayúsculas, sin espacios).
  2. Estandariza fechas a UTC.
  3. Deduplica entre fuentes (transacciones.csv vs histórico).
  4. Resuelve CustomerID nulo -> 'UNKNOWN'.
  5. Clasifica cada línea como VENTA / DEVOLUCION / AJUSTE según Quantity.
  6. Rechaza ventas con precio <= 0 (van a log_rechazos).
  7. Calcula revenue_bruto, revenue_devolucion, revenue_neto.
  8. Resuelve el nombre canónico de producto (moda de Description por StockCode).
  9. Construye natural_key para idempotencia.

Entrada: los parquet que dejó extract.py en data/staging/.
Salida: parquet listos para cargar (dim_producto, dim_cliente,
fact_transacciones, log_rechazos) -- load.py los lee y hace upsert.
"""

import json
import pandas as pd
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
RUTA_STAGING = RAIZ_PROYECTO / "data/staging"

CUSTOMER_ID_DESCONOCIDO = "UNKNOWN"


def _normalizar_codigo_producto(df):
    df["stock_code"] = df["stock_code"].astype(str).str.upper().str.strip()
    return df


def _normalizar_fecha_utc(df):
    # Las fechas crudas no traen zona horaria; el negocio no especificó
    # el huso original, así que se documenta el supuesto: se asume que
    # las fechas ya están en UTC y solo se les pone la etiqueta de zona,
    # sin desplazar la hora. Ver docs/decisiones_tecnicas.md.
    df["fecha_utc"] = pd.to_datetime(df["invoice_date"], errors="coerce", utc=True)
    df["fecha_dia"] = df["fecha_utc"].dt.date
    return df


def _deduplicar_entre_fuentes(df):
    """transacciones.csv y el histórico se solapan en el año 2010-2011
    (ver hallazgo documentado). Se prioriza 'historico' como la fuente
    más completa; si la misma línea aparece en ambas, se descarta la
    versión de 'transacciones_diarias'."""
    orden_prioridad = {"historico": 0, "transacciones_diarias": 1}
    df["_prioridad"] = df["fuente"].map(orden_prioridad)
    df = df.sort_values("_prioridad")

    clave_cruce = ["invoice_no", "stock_code", "quantity", "unit_price"]
    df = df.drop_duplicates(subset=clave_cruce, keep="first")

    return df.drop(columns="_prioridad")


def _resolver_customer_id(df):
    df["customer_id"] = df["customer_id"].apply(
        lambda x: CUSTOMER_ID_DESCONOCIDO if pd.isna(x) else str(int(x))
    )
    df["es_invitado"] = df["customer_id"] == CUSTOMER_ID_DESCONOCIDO
    return df


def _clasificar_tipo_linea(df):
    condiciones = [df["quantity"] > 0, df["quantity"] < 0]
    valores = ["VENTA", "DEVOLUCION"]
    df["tipo_linea"] = pd.Series(
        pd.NA, index=df.index, dtype="object"
    )
    for condicion, valor in zip(condiciones, valores):
        df.loc[condicion, "tipo_linea"] = valor
    df["tipo_linea"] = df["tipo_linea"].fillna("AJUSTE")
    return df


def _separar_rechazos(df):
    """Una venta (Quantity > 0) con precio <= 0 se rechaza por regla de
    negocio explícita. Devoluciones y ajustes no se rechazan por precio,
    porque el precio ahí documenta el valor original de la línea, no una
    venta nueva."""
    es_venta = df["tipo_linea"] == "VENTA"
    precio_invalido = df["unit_price"] <= 0
    mascara_rechazo = es_venta & precio_invalido

    df_rechazados = df[mascara_rechazo].copy()
    df_rechazados["motivo"] = "precio_unitario_no_positivo_en_venta"

    df_validos = df[~mascara_rechazo].copy()
    return df_validos, df_rechazados


def _calcular_revenue(df):
    df["revenue_bruto"] = 0.0
    df["revenue_devolucion"] = 0.0

    es_venta = df["tipo_linea"] == "VENTA"
    es_devolucion = df["tipo_linea"] == "DEVOLUCION"

    df.loc[es_venta, "revenue_bruto"] = (
        df.loc[es_venta, "quantity"] * df.loc[es_venta, "unit_price"]
    )
    # Quantity ya viene negativo en devoluciones; se guarda el valor
    # absoluto en revenue_devolucion para que sea legible como "monto
    # devuelto", no como un negativo confuso.
    df.loc[es_devolucion, "revenue_devolucion"] = (
        df.loc[es_devolucion, "quantity"].abs() * df.loc[es_devolucion, "unit_price"]
    )

    df["revenue_neto"] = df["revenue_bruto"] - df["revenue_devolucion"]
    return df


def _resolver_nombre_canonico(df):
    """Para cada stock_code, el nombre canónico es la descripción más
    frecuente (moda). Si no hay ninguna descripción válida, se usa
    'SIN_DESCRIPCION'."""
    df["description"] = df["description"].fillna("").str.strip()

    def moda_no_vacia(serie):
        serie_valida = serie[serie != ""]
        if serie_valida.empty:
            return "SIN_DESCRIPCION"
        return serie_valida.mode().iloc[0]

    nombres_canonicos = (
        df.groupby("stock_code")["description"]
        .apply(moda_no_vacia)
        .rename("nombre_canonico")
        .reset_index()
    )
    return nombres_canonicos


def _construir_natural_key(df):
    df["natural_key"] = (
        df["fuente"] + "|" +
        df["invoice_no"].astype(str) + "|" +
        df["stock_code"].astype(str) + "|" +
        df["customer_id"].astype(str) + "|" +
        df["fecha_utc"].astype(str) + "|" +
        df["quantity"].astype(str) + "|" +
        df["unit_price"].astype(str)
    )
    return df


def transformar_y_validar(**context):
    ti = context["ti"]
    info_ventas = ti.xcom_pull(task_ids="extraer_ventas_diarias")
    info_historico = ti.xcom_pull(task_ids="extraer_historico")

    df_ventas = pd.read_parquet(info_ventas["ruta"])
    df_historico = pd.read_parquet(info_historico["ruta"])

    df = pd.concat([df_ventas, df_historico], ignore_index=True)
    filas_leidas = len(df)

    df = _normalizar_codigo_producto(df)
    df = _normalizar_fecha_utc(df)
    df = _deduplicar_entre_fuentes(df)
    df = _resolver_customer_id(df)
    df = _clasificar_tipo_linea(df)

    df_validos, df_rechazados = _separar_rechazos(df)
    df_validos = _calcular_revenue(df_validos)
    df_validos = _construir_natural_key(df_validos)

    dim_producto = _resolver_nombre_canonico(df_validos)
    dim_producto["categoria"] = "SIN_CATEGORIA"
    dim_producto["activo"] = True

    dim_cliente = (
        df_validos.groupby("customer_id")
        .agg(pais=("country", "first"), es_invitado=("es_invitado", "first"))
        .reset_index()
    )

    fact_transacciones = df_validos[[
        "natural_key", "invoice_no", "stock_code", "customer_id",
        "fecha_utc", "fecha_dia", "country", "quantity", "unit_price",
        "revenue_bruto", "revenue_devolucion", "revenue_neto",
        "tipo_linea", "fuente",
    ]].rename(columns={"country": "pais", "quantity": "cantidad",
                        "unit_price": "precio_unitario"})

    df_rechazados = df_rechazados.copy()
    df_rechazados["payload_original"] = df_rechazados.apply(
        lambda fila: json.dumps(fila.drop(labels=["motivo"]).astype(str).to_dict()),
        axis=1,
    )
    log_rechazos = df_rechazados[[
        "fuente", "invoice_no", "stock_code", "motivo", "payload_original",
    ]]

    RUTA_STAGING.mkdir(parents=True, exist_ok=True)
    ruta_dim_producto = RUTA_STAGING / "dim_producto.parquet"
    ruta_dim_cliente = RUTA_STAGING / "dim_cliente.parquet"
    ruta_fact = RUTA_STAGING / "fact_transacciones.parquet"
    ruta_rechazos = RUTA_STAGING / "log_rechazos.parquet"

    dim_producto.to_parquet(ruta_dim_producto, index=False)
    dim_cliente.to_parquet(ruta_dim_cliente, index=False)
    fact_transacciones.to_parquet(ruta_fact, index=False)
    log_rechazos.to_parquet(ruta_rechazos, index=False)

    return {
        "filas_leidas": filas_leidas,
        "filas_validas": len(fact_transacciones),
        "filas_rechazadas": len(log_rechazos),
        "ruta_dim_producto": str(ruta_dim_producto),
        "ruta_dim_cliente": str(ruta_dim_cliente),
        "ruta_fact": str(ruta_fact),
        "ruta_rechazos": str(ruta_rechazos),
    }


if __name__ == "__main__":
    # Simula el contexto de Airflow para poder probar localmente,
    # encadenado después de correr extract.py.
    from extract import extraer_ventas_diarias, extraer_historico

    info_1 = extraer_ventas_diarias()
    info_2 = extraer_historico()

    class _TiFalso:
        def xcom_pull(self, task_ids):
            return info_1 if task_ids == "extraer_ventas_diarias" else info_2

    resultado = transformar_y_validar(ti=_TiFalso())
    print(resultado)