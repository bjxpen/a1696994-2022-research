"""Script for blacklisting already analyzed repositories.

This clones all rows from the sample table to the blacklist table.
The next thing you do is run the sampling script again.
"""

from database import repo_first_sample_table, repo_sample_blacklist_table


def clone_sample_to_blacklist():
    src = repo_first_sample_table
    dest = repo_sample_blacklist_table
    with src.connect_database():
        dest.create()
        src.copy_to_table(dest)

if __name__ == "__main__":
    clone_sample_to_blacklist()
