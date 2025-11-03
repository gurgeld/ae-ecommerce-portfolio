{{ config(schema='staging', alias='stg_customers') }}

SELECT * FROM {{ source('raw','customers') }}