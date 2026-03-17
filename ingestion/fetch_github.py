# fetch_github.py
# Fetches developer activity data from the GitHub API
# for the public repositories of our 5 tracked companies.
#
# What we collect per company:
# - Commit count (how active are developers?)
# - Open pull requests (how much code review is happening?)
# - Stars (is community interest growing?)
# - Number of contributors (how many people are active?)
#
# This data is the "developer activity" side of our correlation analysis.

import time
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
import logging

from ingestion.utils import (
    get_logger,
    get_env_variable,
    save_locally,
    file_already_exists,
    get_today
)

# Set up logger for this script
logger = get_logger("fetch_github")

# GitHub API base URL
BASE_URL = "https://api.github.com"

# Map each stock ticker to its main GitHub organization
# These are the official GitHub orgs for each company
# Map each stock ticker to its main GitHub organization
COMPANY_GITHUB_MAP = {
    "MSFT": "microsoft",
    "GOOGL": "google",
    "META": "facebook",
    "AAPL": "apple",
    "AMZN": "aws"      # Fixed: Amazon's main GitHub org is 'aws' not 'amzn'
}

# The specific repos we track per company
# We pick the most active/representative public repos
COMPANY_REPOS = {
    "MSFT": ["vscode", "TypeScript", "terminal"],
    "GOOGL": ["material-design-icons", "guava", "gson"],
    "META": ["react", "react-native", "folly"],  # Fixed: pytorch moved to pytorch org
    "AAPL": ["swift-nio", "swift-collections", "swift-algorithms"],
    "AMZN": ["aws-cli", "aws-sdk-js-v3", "aws-cdk"]  # Fixed: correct repo names
}


def get_headers(token):
    """
    Builds the request headers for GitHub API calls.
    Using a token increases rate limit from 60 to 5000 requests/hour.
    """
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=16),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def fetch_repo_stats(org, repo, headers):
    """
    Fetches basic stats for a single GitHub repository.
    Returns stars, forks, open issues, and watchers.

    Args:
        org: GitHub organization name e.g. 'microsoft'
        repo: repository name e.g. 'vscode'
        headers: request headers including auth token

    Returns:
        dict: repository stats
    """
    url = f"{BASE_URL}/repos/{org}/{repo}"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=16),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def fetch_recent_commits(org, repo, headers, since_date):
    """
    Fetches the number of commits made to a repo since a given date.
    We use this to measure how active development is day-over-day.

    Args:
        org: GitHub organization name
        repo: repository name
        headers: request headers
        since_date: ISO format date string e.g. '2026-03-16T00:00:00Z'

    Returns:
        int: number of commits since the given date
    """
    url = f"{BASE_URL}/repos/{org}/{repo}/commits"

    params = {
        "since": since_date,
        "per_page": 100  # Max per page — we just need the count
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    commits = response.json()

    # GitHub returns a list of commits — we just count them
    return len(commits)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=16),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def fetch_open_pull_requests(org, repo, headers):
    """
    Fetches the count of currently open pull requests for a repo.
    High PR count = active development and code review.

    Args:
        org: GitHub organization name
        repo: repository name
        headers: request headers

    Returns:
        int: number of open pull requests
    """
    url = f"{BASE_URL}/repos/{org}/{repo}/pulls"

    params = {
        "state": "open",
        "per_page": 100
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    pulls = response.json()
    return len(pulls)


def fetch_company_activity(ticker, token):
    """
    Aggregates GitHub activity across all tracked repos for one company.
    Combines stats from multiple repos into a single summary.

    Args:
        ticker: stock symbol e.g. 'MSFT'
        token: GitHub personal access token

    Returns:
        dict: aggregated activity metrics for the company
    """
    org = COMPANY_GITHUB_MAP[ticker]
    repos = COMPANY_REPOS[ticker]
    headers = get_headers(token)
    today = get_today()

    # We fetch commits from yesterday onwards
    since_date = f"{today}T00:00:00Z"

    # Aggregate metrics across all repos for this company
    total_commits = 0
    total_open_prs = 0
    total_stars = 0
    total_forks = 0
    repo_details = []

    for repo in repos:
        try:
            logger.info(f"Fetching GitHub data for {org}/{repo}")

            # Get basic repo stats
            repo_stats = fetch_repo_stats(org, repo, headers)
            stars = repo_stats.get("stargazers_count", 0)
            forks = repo_stats.get("forks_count", 0)

            # Get recent commit count
            commit_count = fetch_recent_commits(
                org, repo, headers, since_date
            )

            # Get open PR count
            pr_count = fetch_open_pull_requests(org, repo, headers)

            # Add to totals
            total_commits += commit_count
            total_open_prs += pr_count
            total_stars += stars
            total_forks += forks

            # Save per-repo details
            repo_details.append({
                "repo": repo,
                "stars": stars,
                "forks": forks,
                "commits_today": commit_count,
                "open_prs": pr_count
            })

            # Small delay to respect GitHub rate limits
            time.sleep(1)

        except Exception as e:
            logger.error(f"Error fetching {org}/{repo}: {e}")
            continue

    # Build the final summary for this company
    return {
        "ticker": ticker,
        "github_org": org,
        "date": today,
        "total_commits_today": total_commits,
        "total_open_prs": total_open_prs,
        "total_stars": total_stars,
        "total_forks": total_forks,
        "repos_tracked": len(repos),
        "repo_details": repo_details,
        "_ingested_at": today
    }


def run_github_ingestion():
    """
    Main function that loops through all companies
    and fetches their GitHub activity for today.
    """
    logger.info("Starting GitHub activity ingestion")

    # Load GitHub token from environment
    token = get_env_variable("GITHUB_TOKEN")
    today = get_today()

    # Track results
    success_count = 0
    skip_count = 0
    error_count = 0

    for ticker in COMPANY_GITHUB_MAP.keys():

        filename = f"{ticker}_{today}.json"

        # Idempotency check
        if file_already_exists("github", filename):
            logger.info(
                f"Already fetched GitHub data for {ticker} "
                f"on {today} — skipping"
            )
            skip_count += 1
            continue

        try:
            # Fetch aggregated activity for this company
            data = fetch_company_activity(ticker, token)

            # Save to data/raw/github/
            file_path = save_locally(data, "github", filename)
            logger.info(f"Saved {ticker} GitHub data to {file_path}")
            success_count += 1

        except Exception as e:
            logger.error(f"Failed to fetch GitHub data for {ticker}: {e}")
            error_count += 1

    # Summary
    logger.info(
        f"GitHub ingestion complete. "
        f"Success: {success_count}, "
        f"Skipped: {skip_count}, "
        f"Errors: {error_count}"
    )


if __name__ == "__main__":
    run_github_ingestion()