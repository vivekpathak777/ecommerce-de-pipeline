{{ config(
    materialized='table',
    catalog='demo_ecommerce',
    schema='ecomm_gold'
) }}

-- Combine orders with product prices and payment information
WITH orders AS (
    SELECT *
    FROM {{ source('ecommerce_silver', 'orders') }}
),

products AS (
    SELECT *
    FROM {{ source('ecommerce_silver', 'products') }}
),

payments AS (
    SELECT *
    FROM {{ source('ecommerce_silver', 'payments') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.status AS order_status,
    p.product_name,
    p.category,
    p.price,

    -- Calculate revenue only for completed, successfully paid orders
    CASE
        WHEN o.status = 'Completed'
             AND pay.payment_status = 'Success'
        THEN o.quantity * p.price
        ELSE 0
    END AS revenue,

    pay.payment_method,
    pay.payment_status

FROM orders o
LEFT JOIN products p
    ON o.product_id = p.product_id
LEFT JOIN payments pay
    ON o.order_id = pay.order_id