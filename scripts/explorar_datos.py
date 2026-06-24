import pandas as pd
from pathlib import Path

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)

# Rutas relativas a la raíz del proyecto (NO al directorio donde se
# ejecute el script), para que funcione sin importar desde dónde lo
# llames: python3 explorar_datos.py o python3 scripts/explorar_datos.py
RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
RUTA_VENTAS_DIARIAS = RAIZ_PROYECTO / "data/raw/transacciones.csv"
RUTA_HISTORICO_2009 = RAIZ_PROYECTO / "data/raw/historic_2009.csv"
RUTA_HISTORICO_2010 = RAIZ_PROYECTO / "data/raw/historic_2010.csv"


def explorar_fuente(nombre, df):
    print(f"\n{'='*70}")
    print(f"FUENTE: {nombre}")
    print(f"{'='*70}")

    print(f"\n-- Forma: {df.shape[0]} filas, {df.shape[1]} columnas")
    print(f"\n-- Columnas y tipos:\n{df.dtypes}")

    print(f"\n-- Nulos por columna:\n{df.isnull().sum()}")

    print(f"\n-- Filas totalmente duplicadas: {df.duplicated().sum()}")

    # Detecta la columna de cantidad y precio sin asumir nombre exacto
    col_cantidad = next((c for c in df.columns if "quantity" in c.lower()), None)
    col_precio = next((c for c in df.columns if "price" in c.lower()), None)
    col_codigo = next((c for c in df.columns if "stockcode" in c.lower().replace(" ", "") or "code" in c.lower()), None)
    col_descripcion = next((c for c in df.columns if "description" in c.lower()), None)
    col_cliente = next((c for c in df.columns if "customer" in c.lower()), None)
    col_fecha = next((c for c in df.columns if "date" in c.lower()), None)
    col_pais = next((c for c in df.columns if "country" in c.lower()), None)

    if col_cantidad:
        negativos = (df[col_cantidad] <= 0).sum()
        print(f"\n-- Columna cantidad: '{col_cantidad}'")
        print(f"   Cantidad <= 0 (posibles devoluciones/ajustes): {negativos} ({negativos/len(df)*100:.2f}%)")
        print(f"   Rango: {df[col_cantidad].min()} a {df[col_cantidad].max()}")

    if col_precio:
        invalidos = (df[col_precio] <= 0).sum()
        print(f"\n-- Columna precio: '{col_precio}'")
        print(f"   Precio <= 0: {invalidos} ({invalidos/len(df)*100:.2f}%)")
        print(f"   Rango: {df[col_precio].min()} a {df[col_precio].max()}")

    if col_codigo:
        print(f"\n-- Columna código de producto: '{col_codigo}'")
        print(f"   Códigos únicos: {df[col_codigo].nunique()}")
        # códigos que no son puramente numéricos (empiezan con letra)
        no_numericos = df[col_codigo].astype(str).str.match(r"^[A-Za-z]").sum()
        print(f"   Códigos que empiezan con letra: {no_numericos} ({no_numericos/len(df)*100:.2f}%)")
        print(f"   Ejemplos: {df[col_codigo].astype(str).unique()[:10].tolist()}")

    if col_descripcion and col_codigo:
        variaciones = df.groupby(col_codigo)[col_descripcion].nunique()
        codigos_con_variacion = (variaciones > 1).sum()
        print(f"\n-- Variaciones de descripción para el mismo código: {codigos_con_variacion}")
        # muestra un ejemplo real
        ejemplo_codigo = variaciones[variaciones > 1].index[0] if codigos_con_variacion > 0 else None
        if ejemplo_codigo:
            print(f"   Ejemplo (código {ejemplo_codigo}):")
            print(f"   {df[df[col_codigo] == ejemplo_codigo][col_descripcion].unique().tolist()}")

    if col_cliente:
        nulos_cliente = df[col_cliente].isnull().sum()
        print(f"\n-- Columna cliente: '{col_cliente}'")
        print(f"   Sin customer ID: {nulos_cliente} ({nulos_cliente/len(df)*100:.2f}%)")

    if col_fecha:
        print(f"\n-- Columna fecha: '{col_fecha}'")
        print(f"   Tipo original: {df[col_fecha].dtype}")
        print(f"   Ejemplos crudos: {df[col_fecha].astype(str).unique()[:5].tolist()}")
        fechas_parseadas = pd.to_datetime(df[col_fecha], errors="coerce")
        print(f"   Rango (si parsea bien): {fechas_parseadas.min()} a {fechas_parseadas.max()}")
        print(f"   Filas que NO parsearon como fecha: {fechas_parseadas.isnull().sum()}")

    if col_pais:
        print(f"\n-- Columna país: '{col_pais}'")
        print(f"   Países únicos: {df[col_pais].nunique()}")
        print(f"   Top 5:\n{df[col_pais].value_counts().head()}")


if __name__ == "__main__":
    print("Cargando transacciones.csv (ventas diarias)...")
    df_ventas = pd.read_csv(RUTA_VENTAS_DIARIAS, encoding="ISO-8859-1")
    explorar_fuente("transacciones.csv (ventas diarias)", df_ventas)

    print("\n\nCargando historic_2009.csv + historic_2010.csv (histórico)...")
    df_2009 = pd.read_csv(RUTA_HISTORICO_2009, encoding="ISO-8859-1")
    df_2010 = pd.read_csv(RUTA_HISTORICO_2010, encoding="ISO-8859-1")
    print(f"   historic_2009.csv: {df_2009.shape[0]} filas")
    print(f"   historic_2010.csv: {df_2010.shape[0]} filas")
    df_historico = pd.concat([df_2009, df_2010], ignore_index=True)
    explorar_fuente("histórico (2009 + 2010 unidos)", df_historico)

    print(f"\n{'='*70}")
    print("RESUMEN PARA TU DOCUMENTO DE DECISIONES")
    print(f"{'='*70}")
    print("Copia los números relevantes de arriba para justificar tus")
    print("decisiones sobre: cantidades <= 0, precios <= 0, customer ID nulo,")
    print("variaciones de descripción, y el solape de fechas entre fuentes.")