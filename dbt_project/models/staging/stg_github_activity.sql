-- stg_github_activity.sql
-- Staging model for GitHub developer activity data.
-- Selects only GitHub-related columns.

with source as (

    select * from {{ source('raw', 'raw_combined') }}

),

renamed as (

    select
        -- identifiers
        ticker,
        date,

        -- GitHub activity metrics
        total_commits_today,
        total_open_prs,
        total_stars,
        total_forks,
        repos_tracked

    from source

    -- Only keep rows where we have GitHub data
    where total_commits_today is not null

)

select * from renamed