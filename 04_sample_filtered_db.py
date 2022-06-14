"""Module for sampling repositories from repo metadata db table.

"""
from random import shuffle
from typing import Iterable, Optional
from schema_types import Repository
from database import Table, repo_filtered_table, repo_first_sample_table, repo_sample_blacklist_table
from config import REPO_SAMPLE_MAX_TOTAL_REPO_SIZE, REPO_FIRST_SAMPLE_SIZE

def get_total_repos_size(repos: Iterable[Repository]):
    """Returns the total size (KB) of given repositories.
    
    """
    return sum(repo.kilobytes for repo in repos)

def take_repo_samples(src_table: Table, dest_table: Table, sample_size:int, max_total_repo_size: int, 
    blacklist_table: Optional[Table] = None):
    print(f"Fetching repository records from {src_table.table_name}")

    blacklisted_repo_ids = set()

    if blacklist_table is not None:
        with blacklist_table.connect_database():
            if blacklist_table.is_exists():
                print(f"Fetching blacklisted repos from {blacklist_table.table_name}")
                blacklisted_repo_ids = {Repository.from_tuple(row).id for row in blacklist_table.iterate_rows(one_row_a_time=True)}

    with src_table.connect_database():
        print(f"Fetching all repos from {src_table.table_name}")
        repos = (Repository.from_tuple(row) for row in src_table.iterate_rows(one_row_a_time=True))
        repos = [repo for repo in repos if repo.id not in blacklisted_repo_ids]

    print(f"Sampling (max total repository size: {int(max_total_repo_size/1000)} MB)")
    while True:
        shuffle(repos)
        samples = repos[:sample_size]
        total_size = get_total_repos_size(samples)
        if total_size <= max_total_repo_size:
            break

    print(f"Sample size: {sample_size}, Total MB: {int(total_size/1000)}")
    print(f"Writing samples to table {dest_table.table_name}")
    with dest_table.connect_database():
        dest_table.drop()
        dest_table.create()
        dest_table.add_rows(repo.to_tuple() for repo in samples)
    print("Done")


if __name__ == "__main__":
    sample_size = REPO_FIRST_SAMPLE_SIZE
    max_total_repo_size = REPO_SAMPLE_MAX_TOTAL_REPO_SIZE
    take_repo_samples(
        src_table=repo_filtered_table,
        dest_table=repo_first_sample_table, 
        sample_size=sample_size, 
        max_total_repo_size=max_total_repo_size,
        blacklist_table=repo_sample_blacklist_table,
    )
