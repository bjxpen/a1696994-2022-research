import re
from typing import Dict, Tuple, Pattern
from functools import partial
from database import Table, repo_raw_table, repo_filtered_table
from schema_types import Repository
from config import (
    REPO_RAW_TABLE_RANGE_FILTERS,
    REPO_RAW_TABLE_DESCRIPTION_BLACKLIST,
    REPO_RAW_TABLE_DESCRIPTION_MIN_CHAR,
    REPO_RAW_TABLE_NAME_BLACKLIST,
    REPO_RAW_TABLE_FILTER_MIN_ENG_CHAR_RATIO
)

REPO_RAW_TABLE_DESCRIPTION_BLACKLIST_REGEX = [
    re.compile(f"[^A-Za-z]({bad_word})[^A-Za-z]", re.IGNORECASE)
    for bad_word in REPO_RAW_TABLE_DESCRIPTION_BLACKLIST
]

def build_range_filter_where_clause(filter_settings: Dict[str, Tuple[int, int]]):
    """Builds a numerical-ranged where clause given a dictonary of filter settings
    
    Settings format:
        [   # col_name, col_filter
            "stars": (10, 100),
            ...
        ]
    """
    sql_wheres = []
    for col_name, col_filter in filter_settings.items():
        if isinstance(col_filter, tuple):
            min_val, max_val = col_filter
            sql_expr = f"{col_name} BETWEEN {min_val} and {max_val}"
            sql_wheres.append(sql_expr)
    sql = " AND ".join(sql_wheres)
    return sql

def contains_most_english_chars(text: str, min_ratio: float = 0.5):
    eng_count = sum([65 <= ord(char) <= 90 or 97 <= ord(char) <= 122 for char in text])
    min_count = min(1, max(0, min_ratio)) * len(text)
    return eng_count >= min_count

def filter_text_in_repo(repo: Repository, min_description_len: int, 
    name_blacklist: Tuple[str], description_blacklist_regex: Tuple[Pattern],
    min_descrption_eng_char_ratio: float
):
    """Returns True if given repository passes tests, False otherwise.
    
    """
    name = repo.name
    desp = repo.description

    if desp is None or len(desp) < min_description_len:
        return False

    for bad_name in name_blacklist:
        if bad_name in name:
            return False

    if not contains_most_english_chars(desp, min_ratio = min_descrption_eng_char_ratio):
        return False

    for regex in description_blacklist_regex:
        res = regex.search(repo.description)
        if res is not None and res.group(1) is not None:
            return False
    return True

def generate_filtered_table(src_table: Table, dest_table: Table):
    where_clause = build_range_filter_where_clause(REPO_RAW_TABLE_RANGE_FILTERS)
    with src_table.connect_database():
        print(f"Fetching and filtering (1st pass) from {src_table.table_name}")
        repos = tuple(Repository.from_tuple(row) for row in src_table.iterate_rows(
            where=where_clause,
            one_row_a_time=True))
        print(f"{len(repos)} records fetched")

        filter_func = partial(filter_text_in_repo, 
            min_description_len = REPO_RAW_TABLE_DESCRIPTION_MIN_CHAR,
            name_blacklist = REPO_RAW_TABLE_NAME_BLACKLIST,
            description_blacklist_regex = REPO_RAW_TABLE_DESCRIPTION_BLACKLIST_REGEX,
            min_descrption_eng_char_ratio = REPO_RAW_TABLE_FILTER_MIN_ENG_CHAR_RATIO)

        print("Filtering (2nd pass)")
        filtered_repos = tuple(filter(filter_func, repos))
        print(f"{len(filtered_repos)} records remain")

    print(f"Writing to table {dest_table.table_name}")
    with dest_table.connect_database():
        dest_table.drop()
        dest_table.create()
        dest_table.add_rows(repo.to_tuple() for repo in filtered_repos)

    print("Done")

if __name__ == "__main__":
    generate_filtered_table(
        src_table=repo_raw_table,
        dest_table=repo_filtered_table,
    )
