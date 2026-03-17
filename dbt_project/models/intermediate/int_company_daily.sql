-- int_company_daily.sql
-- Intermediate model that joins all three staging models together
-- on ticker + date to create one unified row per company per day.
--
-- This is where we combine:
--   stock prices + polygon trade data + github activity
--
-- Think of this as the "assembly" layer before analytics.

with stock_prices as (

    select * from {{ ref('stg_stock_prices') }}

),

polygon_trades as (

    select * from {{ ref('stg_polygon_trades') }}

),

github_activity as (

    select * from {{ ref('stg_github_activity') }}

),

-- Join all three sources on ticker and date
-- Stock prices is the base since it has the most complete history
joined as (

    select
        -- identifiers
        sp.ticker,
        sp.date,

        -- stock price data
        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,
        sp.volume,
        sp.daily_return_pct,

        -- polygon trade data (may be null for older dates)
        pt.polygon_open,
        pt.polygon_close,
        pt.polygon_volume,
        pt.vwap,
        pt.transactions,

        -- github activity (may be null for older dates)
        ga.total_commits_today,
        ga.total_open_prs,
        ga.total_stars,
        ga.total_forks,
        ga.repos_tracked,

        -- metadata
        sp.processed_at

    from stock_prices sp

    left join polygon_trades pt
        on sp.ticker = pt.ticker
        and sp.date = pt.date

    left join github_activity ga
        on sp.ticker = ga.ticker
        and sp.date = ga.date

)

select * from joined