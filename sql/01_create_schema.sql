CREATE SCHEMA IF NOT EXISTS datamart;
SET search_path TO datamart;

CREATE TABLE IF NOT EXISTS dim_producto (
    producto_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL UNIQUE,
    nombre TEXT,
    categoria VARCHAR(50) NOT NULL DEFAULT 'SIN_CATEGORIA',
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    fecha_creacion TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fecha_actualizacion TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    pais VARCHAR(100),
    es_invitado BOOLEAN NOT NULL DEFAULT FALSE,
    fecha_creacion TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fecha_actualizacion TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dim_cliente (customer_id, pais, es_invitado)
VALUES ('UNKNOWN', NULL, TRUE)
ON CONFLICT (customer_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_transacciones (
    transaccion_id BIGSERIAL PRIMARY KEY,
    natural_key TEXT NOT NULL UNIQUE,
    invoice_no VARCHAR(50) NOT NULL,
    producto_id BIGINT NOT NULL REFERENCES dim_producto(producto_id),
    cliente_id BIGINT NOT NULL REFERENCES dim_cliente(cliente_id),
    fecha_utc TIMESTAMPTZ NOT NULL,
    fecha_dia DATE NOT NULL,
    pais VARCHAR(100) NOT NULL,
    cantidad INTEGER NOT NULL,
    precio_unitario NUMERIC(18, 4) NOT NULL,
    revenue_bruto NUMERIC(18, 2) NOT NULL DEFAULT 0,
    revenue_devolucion NUMERIC(18, 2) NOT NULL DEFAULT 0,
    revenue_neto NUMERIC(18, 2) NOT NULL DEFAULT 0,
    tipo_linea VARCHAR(20) NOT NULL,
    fuente VARCHAR(50) NOT NULL,
    fecha_carga TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_tipo_linea
        CHECK (tipo_linea IN ('VENTA', 'DEVOLUCION', 'AJUSTE')),
    CONSTRAINT chk_precio_no_negativo
        CHECK (precio_unitario >= 0)
);

CREATE TABLE IF NOT EXISTS log_rechazos (
    rechazo_id BIGSERIAL PRIMARY KEY,
    fuente VARCHAR(50) NOT NULL,
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    motivo TEXT NOT NULL,
    payload_original JSONB NOT NULL,
    fecha_carga TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_auditoria (
    auditoria_id BIGSERIAL PRIMARY KEY,
    proceso VARCHAR(100) NOT NULL,
    fuente VARCHAR(50),
    filas_leidas INTEGER NOT NULL DEFAULT 0,
    filas_validas INTEGER NOT NULL DEFAULT 0,
    filas_insertadas INTEGER NOT NULL DEFAULT 0,
    filas_actualizadas INTEGER NOT NULL DEFAULT 0,
    filas_rechazadas INTEGER NOT NULL DEFAULT 0,
    fecha_inicio TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fecha_fin TIMESTAMPTZ,
    estado VARCHAR(20) NOT NULL,
    mensaje TEXT,
    CONSTRAINT chk_auditoria_estado
        CHECK (estado IN ('INICIADO', 'OK', 'ERROR'))
);

CREATE INDEX IF NOT EXISTS idx_producto_stock_code
    ON dim_producto(stock_code);
CREATE INDEX IF NOT EXISTS idx_cliente_customer_id
    ON dim_cliente(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_fecha_dia
    ON fact_transacciones(fecha_dia);
CREATE INDEX IF NOT EXISTS idx_fact_fecha_utc
    ON fact_transacciones(fecha_utc);
CREATE INDEX IF NOT EXISTS idx_fact_pais
    ON fact_transacciones(pais);
CREATE INDEX IF NOT EXISTS idx_fact_producto
    ON fact_transacciones(producto_id);
CREATE INDEX IF NOT EXISTS idx_fact_cliente
    ON fact_transacciones(cliente_id);
CREATE INDEX IF NOT EXISTS idx_fact_tipo_linea
    ON fact_transacciones(tipo_linea);
CREATE INDEX IF NOT EXISTS idx_fact_fuente
    ON fact_transacciones(fuente);
CREATE INDEX IF NOT EXISTS idx_rechazos_fuente
    ON log_rechazos(fuente);
CREATE INDEX IF NOT EXISTS idx_rechazos_motivo
    ON log_rechazos(motivo);

CREATE OR REPLACE VIEW vw_ventas_detalle AS
SELECT
    f.transaccion_id,
    f.invoice_no,
    c.customer_id,
    c.es_invitado,
    f.fecha_utc,
    f.fecha_dia,
    f.pais,
    p.stock_code,
    p.nombre,
    p.categoria,
    f.cantidad,
    f.precio_unitario,
    f.revenue_bruto,
    f.revenue_devolucion,
    f.revenue_neto,
    f.tipo_linea,
    f.fuente
FROM fact_transacciones f
JOIN dim_producto p ON f.producto_id = p.producto_id
JOIN dim_cliente c ON f.cliente_id = c.cliente_id;

CREATE OR REPLACE VIEW vw_revenue_mensual AS
SELECT
    DATE_TRUNC('month', fecha_utc)::DATE AS mes,
    SUM(revenue_bruto) AS revenue_bruto,
    SUM(revenue_devolucion) AS revenue_devolucion,
    SUM(revenue_neto) AS revenue_neto,
    COUNT(DISTINCT invoice_no) AS cantidad_documentos,
    COUNT(*) AS cantidad_lineas
FROM vw_ventas_detalle
GROUP BY DATE_TRUNC('month', fecha_utc)::DATE;