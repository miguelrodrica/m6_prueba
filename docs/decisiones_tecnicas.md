# Decisiones Tecnicas

Este documento resume las decisiones tomadas para el pipeline ETL de DataMart S.A.S. y deja explicitos los pendientes que no alcanzaron a implementarse.

## Alcance Implementado

Se avanzo en:

- Configuracion base de Airflow con Docker.
- Uso de PostgreSQL interno para metadatos de Airflow.
- Decision de usar PostgreSQL externo como Data Warehouse.
- Montaje de carpetas `dags`, `scripts`, `data` y `sql` dentro de Airflow.
- Exploracion de las fuentes CSV.
- Diseno inicial del modelo analitico en `sql/01_create_schema.sql`.

No se alcanzo a implementar el ETL completo ni las consultas finales de validacion.

## Decision Sobre El Data Warehouse Externo

Aunque el enunciado propone levantar el PostgreSQL analitico dentro de Docker, en esta implementacion se decidio usar un PostgreSQL externo como Data Warehouse.

La razon es separar el runtime del orquestador del repositorio analitico, simulando una arquitectura mas cercana a produccion:

- Airflow mantiene su base de metadatos en Docker.
- El DW vive en una base independiente.
- Airflow se conecta al DW mediante la connection `dw_postgres_conn`.
- Las credenciales se parametrizan por `.env`.

Esta decision mantiene la separacion entre base operacional de Airflow y repositorio analitico, aunque cambia el despliegue sugerido por la prueba.

## Exploracion De Datos

### transacciones.csv

- Filas: 541,909.
- Columnas: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`.
- Duplicados exactos: 5,268.
- `Quantity <= 0`: 10,624 registros.
- `UnitPrice <= 0`: 2,517 registros.
- `CustomerID` nulo: 135,080 registros.
- `Description` nulo: 1,454 registros.
- Rango de fechas: `2010-12-01 08:26:00` a `2011-12-09 12:50:00`.

### Historico 2009 + 2010

- Filas: 1,067,371.
- Columnas: `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, `Country`.
- Duplicados exactos: 34,335.
- `Quantity <= 0`: 22,950 registros.
- `Price <= 0`: 6,207 registros.
- `Customer ID` nulo: 243,007 registros.
- `Description` nulo: 4,382 registros.
- Rango de fechas: `2009-12-01 07:45:00` a `2011-12-09 12:50:00`.

## Modelo De Datos

El modelo definido se encuentra en `sql/01_create_schema.sql`.

### Grano De La Tabla De Hechos

El grano de `fact_transacciones` es una linea de transaccion, equivalente a una fila valida de los CSV.

Se eligio este grano porque cada factura puede contener varios productos. Mantener el nivel de linea permite analizar:

- Producto.
- Categoria.
- Pais.
- Cliente.
- Fecha.
- Devoluciones por producto.
- Revenue bruto y neto.

La tabla no modela cabecera y detalle por separado por limitacion de tiempo. Para calcular metricas por factura, se debe agrupar por `fuente` e `invoice_no`.

### Dimensiones

`dim_producto` almacena:

- `producto_id`: clave surrogate.
- `stock_code`: codigo de producto normalizado.
- `nombre`: nombre canonico tomado de `Description`.
- `categoria`: categoria analitica.
- `activo`: valor por defecto para representar productos vigentes.

`dim_cliente` almacena:

- `cliente_id`: clave surrogate.
- `customer_id`: identificador del cliente.
- `pais`: pais asociado al cliente.
- `es_invitado`: bandera para cliente tecnico sin `CustomerID`.

Se crea una fila tecnica:

```text
customer_id = UNKNOWN
es_invitado = TRUE
```

Esta fila permite cargar transacciones sin `CustomerID` sin perder revenue ni volumen.

## Manejo De Casos Ambiguos

### Transacciones Sin CustomerID

Decision: conservarlas.

Justificacion: representan cerca del 25% de los datos. Excluirlas afectaria fuertemente el analisis de revenue, paises y volumen transaccional.

Tratamiento previsto:

- Si `CustomerID` viene informado, se carga normalmente.
- Si `CustomerID` viene nulo, se asigna a `UNKNOWN`.
- Se marca con `es_invitado = TRUE`.

### Descripciones Inconsistentes

Decision: usar nombre canonico por producto.

Tratamiento previsto:

- Normalizar `Description` eliminando espacios sobrantes.
- Agrupar por `StockCode`.
- Elegir la descripcion mas frecuente como `dim_producto.nombre`.

Si no existe descripcion valida, se puede cargar `NULL` o un valor tecnico como `SIN_DESCRIPCION`.

### Cantidades Menores O Iguales A Cero

Decision: no tratarlas como ventas.

Tratamiento previsto:

- `Quantity > 0`: `tipo_linea = 'VENTA'`.
- `Quantity < 0`: `tipo_linea = 'DEVOLUCION'`.
- `Quantity = 0`: `tipo_linea = 'AJUSTE'` o rechazo, segun validacion final.

El revenue de devolucion se almacenaria como valor positivo en `revenue_devolucion` y se restaria en `revenue_neto`.

### Precio Cero O Negativo

Decision: ventas con precio `<= 0` deben rechazarse.

Tratamiento previsto:

- Para ventas (`Quantity > 0`), si `precio_unitario <= 0`, registrar en `log_rechazos`.
- Guardar el motivo y la fila completa en `payload_original`.

### Duplicados Entre Fuentes

Decision prevista: usar una clave de idempotencia en `fact_transacciones.natural_key`.

La clave debe incluir al menos:

```text
fuente | invoice_no | stock_code | customer_id | fecha_utc | cantidad | precio_unitario
```

Esto permite ejecutar el DAG mas de una vez sin duplicar datos. En `load.py` se deberia usar:

```sql
ON CONFLICT (natural_key) DO UPDATE
```

o:

```sql
ON CONFLICT (natural_key) DO NOTHING
```

La decision final entre actualizar o ignorar dependeria de si se desea reflejar correcciones posteriores del archivo origen.

## Normalizacion De Fuentes

Las fuentes tienen diferencias de nombres:

| Concepto | transacciones.csv | historico |
|---|---|---|
| Factura | `InvoiceNo` | `Invoice` |
| Precio | `UnitPrice` | `Price` |
| Cliente | `CustomerID` | `Customer ID` |
| Producto | `StockCode` | `StockCode` |
| Fecha | `InvoiceDate` | `InvoiceDate` |
| Pais | `Country` | `Country` |

La transformacion pendiente debe mapear ambos formatos a un esquema comun antes de cargar.

## Idempotencia

La idempotencia se garantiza por diseno mediante:

- `fact_transacciones.natural_key UNIQUE`.
- Carga con `UPSERT`.
- Auditoria por corrida en `etl_auditoria`.

Esto aun esta pendiente de implementacion en `scripts/load.py`.

## Calidad De Datos

Registros rechazables previstos:

- `StockCode` vacio.
- Fecha no parseable.
- Venta con precio `<= 0`.
- Cantidad nula.
- Precio nulo.
- Producto sin codigo normalizable.

Cada rechazo debe conservar:

- Fuente.
- Invoice.
- StockCode.
- Motivo.
- Fila original como JSON.

## Categorias

El documento de la prueba define cinco categorias de negocio:

- Electronica.
- Hogar.
- Ropa.
- Deportes.
- Papeleria.

Los CSV no traen categoria. En esta version, `dim_producto.categoria` tiene un valor por defecto `SIN_CATEGORIA`.

Pendiente recomendado:

- Crear una tabla `dim_categoria`.
- Poblarla con las cinco categorias del negocio y `SIN_CATEGORIA`.
- Clasificar productos por API opcional, archivo seed o heuristica por palabras clave.

## Diagrama Del Modelo

Representacion textual:

```text
dim_producto
  producto_id PK
  stock_code UNIQUE
  nombre
  categoria

dim_cliente
  cliente_id PK
  customer_id UNIQUE
  pais
  es_invitado

fact_transacciones
  transaccion_id PK
  natural_key UNIQUE
  producto_id FK -> dim_producto.producto_id
  cliente_id FK -> dim_cliente.cliente_id
  invoice_no
  fecha_utc
  fecha_dia
  pais
  cantidad
  precio_unitario
  revenue_bruto
  revenue_devolucion
  revenue_neto
  tipo_linea
  fuente

log_rechazos
  rechazo_id PK
  fuente
  invoice_no
  stock_code
  motivo
  payload_original

etl_auditoria
  auditoria_id PK
  proceso
  fuente
  metricas de filas
  estado
```

## Pendientes

Para completar la prueba se debe implementar:

- Extraccion de fuentes CSV.
- Transformacion y normalizacion.
- Validaciones de calidad.
- Carga idempotente al DW externo.
- DAG completo con schedule diario y retries.
- Consultas SQL para las preguntas de negocio.
- Prueba de ejecucion doble para demostrar idempotencia.
