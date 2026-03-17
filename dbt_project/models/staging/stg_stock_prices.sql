-- stg_stock_prices.sql
-- Staging model for Alpha Vantage stock price data.
-- Selects only stock-related columns and renames them
-- to clean, consistent names we use throughout the project.
-- No business logic here — just cleaning and renaming.

with source as (

    select * from {{ source('raw', 'raw_combined') }}

),

renamed as (

    select
        -- identifiers
        ticker,
        date,

        -- price data from Alpha Vantage
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        -- calculated in Spark processing
        daily_return_pct,

        -- metadata
        processed_at

    from source

    -- Only keep rows where we have stock price data
    where close_price is not null

)

select * from renamed