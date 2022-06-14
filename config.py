"""Global configuration file

Contains config constants used to control the behaviour of multiple scripts.
"""

DB_PATH = "my_db_stars_min_5.db"
DB_TABLE_FETCH_SIZE = 20000 # rows

GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
GITHUB_ACCOUNT_ACCESS_TOKEN = "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"
GITHUB_GRAPHQL_RATELIMIT_QUERY_PATH = "rate_limit_query.graphql"
REPO_QUERY_GRAPHQL_PATH = "repo_query.graphql"

REPO_QUERY_GRAPHQL_PARAMS = {
    # https://docs.github.com/en/search-github/searching-on-github/searching-for-repositories
    "queryStr": "language:python sort:stars-desc",
    # Github's GraphQL API has a timeout of 10 seconds
    # tune accordingly
    "maxResults": 40,
}
REPO_QUERY_RANGE_PARAMS = {
    # YYYY-MM-DDTHH:MM:SS+00:00 T, followed by HH:MM:SS (hour-minutes-seconds), and a UTC offset (+00:00)
    # e.g. "2008-03-27T00:00:00Z", "2008-03-27T00:00:00+09:30"
    "min_created": "2020-10-06T00:00:00Z",
    "max_created": "2020-10-08T00:00:00Z",
    "min_stars": 5,
    "max_stars": 999999,
    "min_size": 10,
    "max_size": 10000000
}
REPO_QUERY_MAX_PAGE_COUNT = 99999
REPO_QUERY_RETRIES = 2
REPO_QUERY_RATELIMIT_WAIT_SECS = 60 * 5
REPO_QUERY_TIMEOUT_SECS = 20
REPO_RAW_TABLE_NAME = "repo_raw"
REPO_FILTERED_TABLE_NAME = "repo_filtered"
REPO_FIRST_SAMPLE_TABLE_NAME = "repo_sampled_1st"
REPO_SAMPLE_BLACKLIST_TABLE_NAME = "repo_blacklist"
REPO_TABLE_DROP_BEFORE_RUN = False
REPO_INITIAL_GRAPHS_PATH_PREFIX = "repo_initial_graphs"
REPO_SAVE_GRAPHS = True
REPO_TABLE_DISTRIBUTION_GRAPHS = (
    # column  max_value
    ("stars", 10000),
    ("kilobytes", 1000000),
    ("commit_count", 50000),
    ("closed_issue_count", 2000),
)
REPO_RAW_TABLE_RANGE_FILTERS = {
    "stars": (13, 999999),
    # best
    "kilobytes": (32, 30720),
    "commit_count": (28, 99999),
    "closed_issue_count": (3, 99999),
}
REPO_RAW_TABLE_DESCRIPTION_BLACKLIST = ("tutorial course interview challenges deprecated inactive unmaintained I my collection curate awesome list boilerplate "
    "tags dataset font crawl cheat cheatsheet fork example template book notes").split()
REPO_RAW_TABLE_DESCRIPTION_MIN_CHAR = 5
REPO_RAW_TABLE_NAME_BLACKLIST = ("curate awesome list font crawl cheat cheatsheet "
    "fork example template data").split()
REPO_FIRST_SAMPLE_SIZE = 1000
REPO_FIRST_SAMPLE_DIR_PATH = "first_sampled_repos"
REPO_SAMPLE_CLONE_PROGRESS_FILENAME = "progress.txt"
REPO_SAMPLE_MAX_TOTAL_REPO_SIZE = 3500000 # 3.5 GB
REPO_FIRST_SAMPLE_METRICS_FILE_PATH = "first_sampled_repos_metrics.csv"
REPO_RAW_TABLE_FILTER_MIN_ENG_CHAR_RATIO = 0.8


def check_config_prompt():
    """Print all the variables in this config file and prompt to continue.

    Prompt only allows 'y' or 'n' input from stdin, all other inputs are ignored.
    """
    def get_key_vals():
        for prop, value in globals().items():
            if prop.startswith("__"):
                continue
            if prop is func_name:
                continue
            yield prop, value

    import inspect, json
    func_name = inspect.currentframe().f_code.co_name
    print("Below is the Config in config.py:")
    print(json.dumps(dict(get_key_vals()), indent=4, sort_keys=True), "\n")
    while True:
        choice = input("Continue? (y/n) ").strip().lower()
        if choice == "y":
            return True
        if choice == "n":
            return False
