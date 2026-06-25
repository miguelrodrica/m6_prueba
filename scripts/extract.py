"""
Extracción de las fuentes de DataMart.

Decisión de diseño (ver docs/decisiones_tecnicas.md): cada función escribe
su resultado a un archivo parquet en data/staging/ y devuelve solo metadatos
ligeros (ruta + conteo de filas). Esto es lo que se empuja a XCom -- nunca
el DataFrame completo, porque XCom no está pensado para mover cientos de
miles de filas entre tareas.

Normalización aplicada aquí (solo nombres de columna, NO reglas de negocio
todavía -- eso es trabajo de transform.py):
    transacciones.csv -> InvoiceNo, StockCode, Description, Quantity,
                          InvoiceDate, UnitPrice, CustomerID, Country
    histórico         -> Invoice,   StockCode, Description, Quantity,
                          InvoiceDate, Price,     Customer ID, Country

Ambas se llevan al mismo esquema común:
    invoice_no, stock_code, description, quantity, invoice_date,
    unit_price, customer_id, country, fuente
"""

import pandas as pd
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
RUTA_VENTAS_DIARIAS = RAIZ_PROYECTO / "data/raw/transacciones.csv"
RUTA_HISTORICO_2009 = RAIZ_PROYECTO / "data/raw/historic_2009.csv"
RUTA_HISTORICO_2010 = RAIZ_PROYECTO / "data/raw/historic_2010.csv"

RUTA_STAGING = RAIZ_PROYECTO / "data/staging"

COLUMNAS_COMUNES = [
    "invoice_no", "stock_code", "description", "quantity",
    "invoice_date", "unit_price", "customer_id", "country", "fuente",
]


def _asegurar_staging():
    RUTA_STAGING.mkdir(parents=True, exist_ok=True)


def extraer_ventas_diarias():
    """Lee transacciones.csv y lo deja normalizado en staging."""
    df = pd.read_csv(RUTA_VENTAS_DIARIAS, encoding="ISO-8859-1")

    df = df.rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
    })
    df["fuente"] = "transacciones_diarias"
    df = df[COLUMNAS_COMUNES]

    _asegurar_staging()
    ruta_salida = RUTA_STAGING / "ventas_diarias.parquet"
    df.to_parquet(ruta_salida, index=False)

    return {"ruta": str(ruta_salida), "filas": len(df)}


def extraer_historico():
    """Lee historic_2009.csv + historic_2010.csv, los une, normaliza
    columnas y deja un único parquet en staging."""
    df_2009 = pd.read_csv(RUTA_HISTORICO_2009, encoding="ISO-8859-1")
    df_2010 = pd.read_csv(RUTA_HISTORICO_2010, encoding="ISO-8859-1")
    df = pd.concat([df_2009, df_2010], ignore_index=True)

    df = df.rename(columns={
        "Invoice": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "unit_price",
        "Customer ID": "customer_id",
        "Country": "country",
    })
    df["fuente"] = "historico"
    df = df[COLUMNAS_COMUNES]

    _asegurar_staging()
    ruta_salida = RUTA_STAGING / "historico.parquet"
    df.to_parquet(ruta_salida, index=False)

    return {"ruta": str(ruta_salida), "filas": len(df)}


if __name__ == "__main__":
    # Permite probar el script de forma aislada, sin Airflow,
    # antes de conectarlo al DAG.
    resultado_1 = extraer_ventas_diarias()
    print(f"transacciones.csv -> {resultado_1}")

    resultado_2 = extraer_historico()
    print(f"histórico -> {resultado_2}")