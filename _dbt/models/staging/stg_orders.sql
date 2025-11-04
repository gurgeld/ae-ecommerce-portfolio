{{ config(schema='staging', alias='stg_orders') }}

SELECT * FROM {{ source('raw','orders') }}