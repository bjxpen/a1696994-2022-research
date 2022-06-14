"""Graph generation, saving and preview

"""

import seaborn
import matplotlib.pyplot as plt

from database import Table, repo_raw_table
from config import (
    REPO_INITIAL_GRAPHS_PATH_PREFIX,
    REPO_SAVE_GRAPHS,
    REPO_TABLE_DISTRIBUTION_GRAPHS,
)

def repo_graphs(table = repo_raw_table,
    save_files = REPO_SAVE_GRAPHS
):
    """Show graphs for initial repo scrapping

    """
    graphs = REPO_TABLE_DISTRIBUTION_GRAPHS
    plt.switch_backend("TkAgg") # tkinter

    if save_files:
        for (col_name, max_col_value) in graphs:
            fig, axes = plt.subplots(ncols=2)
            make_col_graphs(axes, table,
                col_name, max_col_value = max_col_value,
                filter_col_value=True, hist_x_log_scale = True)
            print("Saving files")
            fig.tight_layout()
            plt.savefig(f"{REPO_INITIAL_GRAPHS_PATH_PREFIX}_{col_name}.png")
            plt.savefig(f"{REPO_INITIAL_GRAPHS_PATH_PREFIX}_{col_name}.pdf")
            plt.close()

    fig, axes = plt.subplots(nrows = len(graphs), ncols=2)
    for index, (col_name, max_col_value) in enumerate(graphs):
        _axes = axes if len(graphs) == 1 else axes[index]
        make_col_graphs(_axes, table,
            col_name, max_col_value = max_col_value,
            filter_col_value=True, hist_x_log_scale = True)

    print("Showing")
    mng = plt.get_current_fig_manager()
    mng.window.state("zoomed")
    # plt.subplots_adjust(hspace=0.4)
    fig.tight_layout()
    if save_files:
        print("Saving files")
        plt.savefig(f"{REPO_INITIAL_GRAPHS_PATH_PREFIX}.png")
        plt.savefig(f"{REPO_INITIAL_GRAPHS_PATH_PREFIX}.pdf")
    plt.show()

def make_col_graphs(axes, table: Table, col_name:str, max_col_value: int,
    filter_col_value = False, hist_x_log_scale = False):
    print(f"Fetching {col_name}")
    with table.connect_database():
        cols = tuple(table.iterate_rows(select=col_name,
        one_row_a_time=True, first_col_only=True))

    if filter_col_value:
        print(f"Filtering {col_name}")
        cols = tuple(filter(lambda val: val <= max_col_value, cols))

    print(f"Preparing {col_name} histogram (<= 20 seconds)")
    axis = seaborn.histplot(x=cols, ax=axes[0])
    axis.set(xlabel=col_name, ylabel="Repo Count")
    if hist_x_log_scale:
        axis.set_xscale("log")
    axis.set_yscale("log")

    print(f"Preparing {col_name} boxenplot")
    axis = seaborn.boxenplot(data=cols, ax=axes[1])
    axis.set(xlabel="Repo Count", ylabel=col_name)
    axis.set_yscale("log")

if __name__ == "__main__":
    repo_graphs()
