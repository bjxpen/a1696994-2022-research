import shutil
import os
import stat
import sys
import traceback

from pathlib import Path
from typing import Union

from ratelimit import limits
from pygit2 import clone_repository, RemoteCallbacks, GitError
from natsort import natsorted

from schema_types import Repository
from database import Table, repo_first_sample_table
from config import REPO_FIRST_SAMPLE_DIR_PATH, REPO_SAMPLE_CLONE_PROGRESS_FILENAME

class RepositoryCloneProgressCallback(RemoteCallbacks):
    @limits(calls=1, period=1, raise_on_limit=False)
    def transfer_progress(self, stats):
        print(f"{stats.indexed_objects}/{stats.total_objects}", end="\r")

def remove_readonly(action, path, exc):
    """Remove readonly file in Windows, error handler for shutil.rmtree
    
    """
    os.chmod(path, stat.S_IWRITE)
    os.remove(path)

class _ProgressFile:
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.last_index = int(self.file_path.read_text())
        except:
            self.last_index = -1
    
    def update_index(self, index):
        self.last_index = index
        with self.file_path.open("w") as file:
            file.write(str(self.last_index))

    def is_index_downloaded(self, index):
        return index <= self.last_index

def clone_repos(src_table: Table, 
    root_dir_path: Union[str, Path], 
    progress_filename: str,
    resume_progress=True):
    """Read repository metadata from a given database table and clone each into a folder
    
    Repositories are cloned in natural sort order.
    Each repository name is formatted as "repo_name@repo_owner"
    Existing repository path will be deleted before cloning due to limitation of git clone.
    """
    root_dir_path = Path(root_dir_path)
    root_dir_path.mkdir(parents=True, exist_ok=True)

    cwd = os.getcwd()
    progress = _ProgressFile(root_dir_path.joinpath(progress_filename))

    with src_table.connect_database():
        repos = tuple(Repository.from_tuple(row) for row in
            src_table.iterate_rows(one_row_a_time=True))
        repos = natsorted(repos, key=lambda repo: f"{repo.name}@{repo.owner}")
        repo_count = len(repos)

        for index, repo in enumerate(repos):
            repo_url = repo.get_url()
            dest_folder = f"{repo.name}@{repo.owner}"
            dest_dir_path_abs = root_dir_path.joinpath(dest_folder).absolute()
            dest_dir_path_rel = dest_dir_path_abs.relative_to(cwd)

            if resume_progress and progress.is_index_downloaded(index):
                continue

            if dest_dir_path_abs.exists():
                print(f"Save path {dest_dir_path_rel} already exists, deleting...", end="\r")
                shutil.rmtree(dest_dir_path_abs, onerror=remove_readonly)

            progress_prefix = f"{index + 1}/{repo_count}"
            print(f"{progress_prefix} Cloning {repo.get_name_with_owner()} to {dest_dir_path_rel}")
            try:
                clone_repository(repo_url, str(dest_dir_path_abs),
                    callbacks=RepositoryCloneProgressCallback())
                
            except KeyboardInterrupt:
                sys.exit() 
            except GitError:
                try:
                    shutil.rmtree(dest_dir_path_abs, onerror=remove_readonly)
                except FileNotFoundError:
                    pass
                print(f"Error: Failed cloning {repo.get_name_with_owner()}")
                print(traceback.format_exc())
            progress.update_index(index)

if __name__ == "__main__":
    root_dir_path = Path(__file__).parent.joinpath(REPO_FIRST_SAMPLE_DIR_PATH)
    clone_repos(repo_first_sample_table, root_dir_path, REPO_SAMPLE_CLONE_PROGRESS_FILENAME)
