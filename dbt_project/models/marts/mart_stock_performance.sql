-- mart_stock_performance.sql
-- Final analytics model for stock price performance.
-- This is what Tableau connects to for price charts.
--
-- Adds moving averages and volatility metrics
-- on top of the intermediate model.

with company_daily as (

    select * from {{ ref('int_company_daily') }}

),

-- Add 7-day and 20-day moving averages using window functions
with_moving_averages as (

    select
        ticker,
        date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        daily_return_pct,
        vwap,
        transactions,

        -- 7-day moving average of closing price
        round(
            avg(close_price) over (
                partition by ticker
                order by date
                rows between 6 preceding and current row
            ), 4
        ) as moving_avg_7d,

        -- 20-day moving average of closing price
        round(
            avg(close_price) over (
                partition by ticker
                order by date
                rows between 19 preceding and current row
            ), 4
        ) as moving_avg_20d,

        -- 7-day rolling volatility (standard deviation of returns)
        round(
            stddev(daily_return_pct) over (
                partition by ticker
                order by date
                rows between 6 preceding and current row
            ), 4
        ) as volatility_7d,

        processed_at

    from company_daily

)

select * from with_moving_averages