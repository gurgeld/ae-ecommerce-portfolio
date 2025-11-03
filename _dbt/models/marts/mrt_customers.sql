{{ config(schema='marts', alias='mrt_customers') }}

SELECT * FROM {{ ref('stg_customers') }}