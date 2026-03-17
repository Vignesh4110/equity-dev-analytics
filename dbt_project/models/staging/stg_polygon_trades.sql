-- stg_polygon_trades.sql
-- Staging model for Polygon.io trade data.
-- Selects only Polygon-related columns.
-- Polygon data only exists for dates we ingested
-- so most rows will be null — that's expected.

with source as (

    select * from {{ source('raw', 'raw_combined') }}

),

renamed as (

    select
        -- identifiers
        ticker,
        date,

        -- price data from Polygon
        poly_open       as polygon_open,
        poly_high       as polygon_high,
        poly_low        as polygon_low,
        poly_close      as polygon_close,
        poly_volume     as polygon_volume,

        -- additional Polygon metrics
        vwap,
        transactions

    from source

    -- Only keep rows where we actually have Polygon data
    where vwap is not null

)

select * from renamed