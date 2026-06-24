# DataMart S.A.S. - Pipeline ETL con Airflow y PostgreSQL

Proyecto de prueba para construir la primera version de una plataforma de datos para DataMart S.A.S. El objetivo es procesar transacciones historicas de ecommerce, normalizar los datos y dejarlos disponibles en un repositorio analitico PostgreSQL.

## Estado Actual

El proyecto contiene la base de configuracion de Airflow en Docker, los datos crudos, el script de exploracion y el DDL inicial del repositorio analitico.

Por tiempo, el pipeline ETL completo no quedo implementado. Los archivos `scripts/extract.py`, `scripts/transform.py`, `scripts/load.py` y `scripts/quality_checks.py` estan creados como estructura, pero aun no tienen logica. El DAG `dags/datamart_pipeline_dag.py` contiene la intencion de orquestacion, pero no esta completo porque depende de esas funciones pendientes.

## Arquitectura Definida

- Apache Airflow corre en Docker.
- Airflow usa una base PostgreSQL interna para metadatos.
- El Data Warehouse se conecta como PostgreSQL externo mediante Airflow Connection.
- Los CSV se montan dentro del contenedor en `/opt/airflow/data`.
- El DDL del DW esta en `sql/01_create_schema.sql`.

La decision de usar un DW externo se tomo para simular un escenario productivo donde el repositorio analitico no vive en el mismo runtime del orquestador. La separacion logica se mantiene: Airflow tiene su base de metadatos y el pipeline apunta a una base analitica independiente.

## Estructura

```text
.
├── airflow_init/
│   └── init_connections_variables.py
├── dags/
│   └── datamart_pipeline_dag.py
├── data/
│   └── raw/
│       ├── transacciones.csv
│       ├── historic_2009.csv
│       └── historic_2010.csv
├── docs/
│   └── decisiones_tecnicas.md
├── scripts/
│   ├── explorar_datos.py
│   ├── extract.py
│   ├── transform.py
│   ├── quality_checks.py
│   └── load.py
├── sql/
│   ├── 01_create_schema.sql
│   └── consultas_validacion.sql
├── docker-compose.yml
├── .env.example
└── README.md
```

## Configuracion

1. Crear el archivo `.env` a partir de `.env.example`.

```bash
cp .env.example .env
```

2. Completar las variables:

```env
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow_pass
AIRFLOW_DB_NAME=airflow_meta
AIRFLOW_FERNET_KEY=<fernet_key>
AIRFLOW_JWT_SECRET=<jwt_secret>
AIRFLOW_ADMIN_USER=<usuario_admin>
AIRFLOW_ADMIN_PASSWORD=<password_admin>
AIRFLOW_ADMIN_FIRSTNAME=<nombre>
AIRFLOW_ADMIN_LASTNAME=<apellido>
AIRFLOW_ADMIN_EMAIL=<correo>

DW_HOST=<host_dw_externo>
DW_PORT=<puerto_dw>
DW_DB_NAME=<base_dw>
DW_USER=<usuario_dw>
DW_PASSWORD=<password_dw>
```

3. Levantar Airflow:

```bash
docker compose up
```

Airflow queda disponible en:

```text
http://localhost:8080
```

Durante el inicio, `airflow-init` intenta:

- Migrar la base de metadatos de Airflow.
- Crear la connection `dw_postgres_conn`.
- Crear las variables `precio_minimo_valido`, `pais_default` y `ruta_data_raw`.
- Ejecutar `sql/01_create_schema.sql` contra el DW externo.

## Datos

Las fuentes usadas son:

- `data/raw/transacciones.csv`: dataset diario equivalente a `data.csv`.
- `data/raw/historic_2009.csv`: historico 2009.
- `data/raw/historic_2010.csv`: historico 2010.

El script de exploracion se ejecuta con:

```bash
.venv/bin/python scripts/explorar_datos.py
```

Resultados principales de la exploracion:

| Fuente | Filas | Duplicados | Quantity <= 0 | Precio <= 0 | CustomerID nulo |
|---|---:|---:|---:|---:|---:|
| transacciones.csv | 541,909 | 5,268 | 10,624 | 2,517 | 135,080 |
| historico 2009 + 2010 | 1,067,371 | 34,335 | 22,950 | 6,207 | 243,007 |

Otros hallazgos:

- Hay descripciones nulas e inconsistentes para un mismo `StockCode`.
- Los historicos usan `Invoice`, `Price` y `Customer ID`.
- La fuente diaria usa `InvoiceNo`, `UnitPrice` y `CustomerID`.
- El rango de fechas va desde `2009-12-01` hasta `2011-12-09`.
- Existen transacciones sin cliente, que representan cerca de una cuarta parte de los registros.

## Modelo Analitico

El DDL actual crea el esquema `datamart` con:

- `dim_producto`: catalogo analitico de productos.
- `dim_cliente`: clientes identificados y fila tecnica `UNKNOWN` para registros sin `CustomerID`.
- `fact_transacciones`: tabla de hechos a nivel linea de transaccion.
- `log_rechazos`: filas rechazadas y motivo.
- `etl_auditoria`: control de ejecuciones del pipeline.
- `vw_ventas_detalle`: vista con hechos y dimensiones.
- `vw_revenue_mensual`: vista de revenue mensual.

El grano de `fact_transacciones` es una linea del CSV, no una factura completa. Esto conserva el nivel real de detalle de las fuentes.

## Reglas De Negocio Previstas

- `Quantity > 0`: venta.
- `Quantity <= 0`: devolucion o ajuste.
- Ventas con precio `<= 0`: rechazo.
- Fechas normalizadas a UTC.
- `StockCode` normalizado en mayusculas y sin espacios.
- Revenue bruto: ventas positivas.
- Revenue devolucion: valor absoluto de devoluciones.
- Revenue neto: ventas menos devoluciones.
- Registros sin `CustomerID`: se cargan con cliente tecnico `UNKNOWN`.

## DAG

El DAG previsto tiene estas tareas:

```text
[extraer_ventas_diarias, extraer_historico] >> transformar_y_validar >> cargar_datawarehouse
```

Estado actual:

- La estructura del DAG existe.
- Falta definir el objeto `DAG`, `schedule`, `start_date`, retries y contexto.
- Faltan las funciones importadas desde los scripts ETL.

## Pendientes Para Completar

1. Implementar `scripts/extract.py`.
2. Implementar `scripts/transform.py`.
3. Implementar `scripts/quality_checks.py`.
4. Implementar `scripts/load.py` con `UPSERT` usando `natural_key`.
5. Completar el DAG con definicion formal de Airflow.
6. Probar `docker compose up` desde cero.
7. Ejecutar el DAG dos veces y validar idempotencia.
8. Completar `sql/consultas_validacion.sql`.
9. Crear el diagrama final del modelo.

## Consultas De Validacion

El archivo `sql/consultas_validacion.sql` queda pendiente. Las consultas deben responder:

- Evolucion mensual de ventas netas.
- Revenue bruto y devoluciones por categoria.
- Top 10 productos por revenue neto.
- Top 10 productos por tasa de devolucion.
- Transacciones y ticket promedio por pais.
- Diferencia entre clientes identificados y transacciones sin `CustomerID`.
- Productos con descripcion inconsistente y cantidad de codigos unicos.
- Recomendacion concreta al equipo de producto basada en datos.

## Nota Final

La solucion no quedo completa dentro del tiempo disponible. El avance principal esta en la configuracion base, exploracion de datos y diseno inicial del modelo analitico. La siguiente prioridad tecnica es implementar la transformacion y carga idempotente hacia el DW externo.
