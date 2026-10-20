{{ config(
    materialized='table',
    catalog='demo_ecommerce',
    schema='ecomm_gold'
) }}

-- Aggregate order-level sales into monthly metrics
SELECT
    CAST(DATE_TRUNC('MONTH', order_date) AS DATE) AS sales_month,

    COUNT(*) AS total_orders,

    -- Count only successfully completed orders
    SUM(
        CASE WHEN order_status = 'Completed'
             AND payment_status = 'Success'
        THEN 1 ELSE 0 END
    ) AS completed_orders,

    SUM(revenue) AS total_revenue

FROM {{ ref('fct_sales') }}

GROUP BY DATE_TRUNC('MONTH', order_date)