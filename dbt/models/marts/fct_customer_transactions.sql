-- models/marts/fct_customer_transactions.sql

{{ config(materialized='table') }}

WITH src AS (
    SELECT
        customer_id,
        amount,
        status
    FROM {{ ref('stg_transactions') }}

),

aggregated AS (

    SELECT
        customer_id,
        COUNT(*) AS transaction_count,
        COALESCE(
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END),
        0) AS total_amount_completed,
        COALESCE(SUM(amount), 0) AS total_amount_all
    FROM src
    GROUP BY customer_id

)

SELECT *
FROM aggregated
