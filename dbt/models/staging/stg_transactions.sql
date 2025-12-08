{% set ds_nodash = var('ds') %}
{% set clean_dir = var('clean_dir') %}

{{ config(materialized='table') }}

WITH src AS (

    SELECT *
    FROM read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )

),

typed AS (

    SELECT
        CAST(transaction_id AS VARCHAR)    AS transaction_id,
        CAST(customer_id AS VARCHAR)       AS customer_id,
        CAST(amount AS DOUBLE)             AS amount,
        LOWER(TRIM(status))                AS status,
        CAST(transaction_ts AS TIMESTAMP)  AS transaction_ts
    FROM src

),

validated AS (

    SELECT
        transaction_id,
        customer_id,
        amount,
        status,
        transaction_ts,
        DATE(transaction_ts) AS transaction_date
    FROM typed
    WHERE status IN ('completed', 'pending', 'failed')
        AND amount IS NOT NULL

)

SELECT *
FROM validated
