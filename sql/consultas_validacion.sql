-- ============================================================
-- Consultas de validación -- una por cada pregunta de negocio
-- de la sección 7 del documento. Ejecutables directamente
-- contra el esquema "datamart" después de correr el pipeline.
-- ============================================================

SET search_path TO datamart;

-- ------------------------------------------------------------
-- 1. Evolución mensual de las ventas netas (descontando devoluciones)
-- ------------------------------------------------------------
SELECT
    DATE_TRUNC('month', fecha_utc)::DATE AS mes,
    SUM(revenue_bruto) AS revenue_bruto,
    SUM(revenue_devolucion) AS revenue_devolucion,
    SUM(revenue_neto) AS revenue_neto
FROM fact_transacciones
GROUP BY DATE_TRUNC('month', fecha_utc)::DATE
ORDER BY mes;


-- ------------------------------------------------------------
-- 2. Categorías con más revenue bruto y mayor proporción de devoluciones
-- ------------------------------------------------------------
SELECT
    p.categoria,
    SUM(f.revenue_bruto) AS revenue_bruto_total,
    SUM(f.revenue_devolucion) AS revenue_devolucion_total,
    ROUND(
        SUM(f.revenue_devolucion) / NULLIF(SUM(f.revenue_bruto), 0) * 100,
        2
    ) AS proporcion_devolucion_pct
FROM fact_transacciones f
JOIN dim_producto p ON f.producto_id = p.producto_id
GROUP BY p.categoria
ORDER BY revenue_bruto_total DESC;


-- ------------------------------------------------------------
-- 3a. Top 10 productos con mayor revenue neto
-- ------------------------------------------------------------
SELECT
    p.stock_code,
    p.nombre,
    SUM(f.revenue_neto) AS revenue_neto_total
FROM fact_transacciones f
JOIN dim_producto p ON f.producto_id = p.producto_id
GROUP BY p.stock_code, p.nombre
ORDER BY revenue_neto_total DESC
LIMIT 10;

-- 3b. Top 10 productos con mayor tasa de devolución
-- (tasa = unidades devueltas / unidades vendidas, solo productos con ventas)
SELECT
    p.stock_code,
    p.nombre,
    SUM(CASE WHEN f.tipo_linea = 'VENTA' THEN f.cantidad ELSE 0 END) AS unidades_vendidas,
    SUM(CASE WHEN f.tipo_linea = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END) AS unidades_devueltas,
    ROUND(
        SUM(CASE WHEN f.tipo_linea = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN f.tipo_linea = 'VENTA' THEN f.cantidad ELSE 0 END), 0) * 100,
        2
    ) AS tasa_devolucion_pct
FROM fact_transacciones f
JOIN dim_producto p ON f.producto_id = p.producto_id
GROUP BY p.stock_code, p.nombre
HAVING SUM(CASE WHEN f.tipo_linea = 'VENTA' THEN f.cantidad ELSE 0 END) > 0
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 4. Países que concentran más transacciones y ticket promedio por país
-- ------------------------------------------------------------
SELECT
    f.pais,
    COUNT(DISTINCT f.invoice_no) AS cantidad_facturas,
    COUNT(*) AS cantidad_lineas,
    ROUND(AVG(f.revenue_bruto), 2) AS ticket_promedio_por_linea,
    ROUND(
        SUM(f.revenue_bruto) / NULLIF(COUNT(DISTINCT f.invoice_no), 0),
        2
    ) AS ticket_promedio_por_factura
FROM fact_transacciones f
WHERE f.tipo_linea = 'VENTA'
GROUP BY f.pais
ORDER BY cantidad_facturas DESC;


-- ------------------------------------------------------------
-- 5. Comportamiento de compra: clientes identificados vs sin CustomerID
-- ------------------------------------------------------------
SELECT
    c.es_invitado,
    COUNT(DISTINCT f.invoice_no) AS cantidad_facturas,
    COUNT(*) AS cantidad_lineas,
    ROUND(AVG(f.revenue_bruto), 2) AS ticket_promedio_por_linea,
    SUM(f.revenue_neto) AS revenue_neto_total
FROM fact_transacciones f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
WHERE f.tipo_linea = 'VENTA'
GROUP BY c.es_invitado;


-- ------------------------------------------------------------
-- 6a. Productos con descripción inconsistente (nombre canónico = SIN_DESCRIPCION
--     significa que ninguna fuente trajo una descripción válida para ese código)
-- ------------------------------------------------------------
SELECT stock_code, nombre, categoria
FROM dim_producto
WHERE nombre = 'SIN_DESCRIPCION'
ORDER BY stock_code;

-- 6b. Cantidad total de códigos de producto únicos
SELECT COUNT(*) AS total_codigos_unicos
FROM dim_producto;


-- ------------------------------------------------------------
-- 7. Recomendación al equipo de producto (insumo para la recomendación
--    final del documento de decisiones -- combina revenue y devoluciones)
-- ------------------------------------------------------------
SELECT
    p.stock_code,
    p.nombre,
    p.categoria,
    SUM(f.revenue_bruto) AS revenue_bruto,
    SUM(f.revenue_devolucion) AS revenue_devolucion,
    SUM(f.revenue_neto) AS revenue_neto,
    ROUND(
        SUM(f.revenue_devolucion) / NULLIF(SUM(f.revenue_bruto), 0) * 100, 2
    ) AS tasa_devolucion_revenue_pct
FROM fact_transacciones f
JOIN dim_producto p ON f.producto_id = p.producto_id
GROUP BY p.stock_code, p.nombre, p.categoria
HAVING SUM(f.revenue_bruto) > 0
ORDER BY tasa_devolucion_revenue_pct DESC, revenue_bruto DESC
LIMIT 15;
-- Revisa los primeros resultados: productos con revenue bruto alto Y
-- tasa de devolución alta son los candidatos a tu recomendación final
-- (ej. "el producto X genera $Y en ventas pero devuelve Z%, revisar Q").