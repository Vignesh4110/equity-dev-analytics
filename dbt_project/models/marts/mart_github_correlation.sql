-- mart_github_correlation.sql
-- Final analytics model that brings together stock performance
-- and GitHub developer activity for correlation analysis.
-- This is the main analytical table — the whole point of this project.
--
-- Tableau uses this to show whether developer activity
-- correlates with stock price movements.

with company_daily as (

    select * from {{ ref('int_company_daily') }}

),

-- Only include rows where we have both stock AND github data
-- so our correlation analysis is based on complete records
with_activity as (

    select
        ticker,
        date,

        -- stock metrics
        close_price,
        daily_return_pct,
        volume,
        vwap,

        -- github activity metrics
        total_commits_today,
        total_open_prs,
        total_stars,
        total_forks,

        -- simple activity score
        -- combines commits and PRs into one number
        -- higher score = more developer activity that day
        coalesce(total_commits_today, 0)
            + coalesce(total_open_prs, 0) as developer_activity_score,

        -- flag days where developer activity was unusually high
        -- we define "high" as more than 10 commits in a day
        case
            when total_commits_today > 10 then true
            else false
        end as is_high_activity_day,

        processed_at

    from company_daily

    -- Only keep rows where we have stock price data
    where close_price is not null

)

select * from with_activity