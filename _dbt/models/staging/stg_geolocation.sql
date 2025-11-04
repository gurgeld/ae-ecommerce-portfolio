{{ config(schema='staging', alias='stg_geolocation') }}

SELECT * FROM {{ source('raw','geolocation') }}