from copy import deepcopy
from typing import Optional, Union
from enum import Enum
from timeit import default_timer as get_current_time
from pathlib import Path
from functools import partial
from datetime import datetime, timedelta, timezone

from dateutil.parser import parse as dateutil_parse
from dateutil.relativedelta import relativedelta as dateutil_delta
from aiolimiter import AsyncLimiter
import anyio
import httpx

from schema_types import QueryPage
from database import repo_raw_table
from config import *

headers={
    "Authorization": f"bearer {GITHUB_ACCOUNT_ACCESS_TOKEN}",
    "Content-Type": "application/json",
}

limits = httpx.Limits(
    max_keepalive_connections = 1,
    max_connections = 1,
)

async def fetch_quota(client_post, print_results=True):
    payload = {
        "query": Path(GITHUB_GRAPHQL_RATELIMIT_QUERY_PATH).read_text(),
        "variables": {},
    }
    resp_dict = await client_post(json = payload).json()
    try:
        rate_limit_dict  = resp_dict["data"]["rateLimit"]
        quota = rate_limit_dict["remaining"]
        next_refresh = rate_limit_dict["resetAt"]
        if print_results:
            print(f"Remaining quota: {quota} | Refresh after {next_refresh}")
        return quota, next_refresh
    except KeyError:
        print("Possible rate limit reached with key error.")
        print(resp_dict)
        raise

async def fetch_pages_aiter():
    async with httpx.AsyncClient(limits = limits) as client:
        ctx = _LoopContext()
        client_post = partial(client.post,
            GITHUB_GRAPHQL_ENDPOINT,
            headers = headers,
            timeout = REPO_QUERY_TIMEOUT_SECS,
        )
        quota = 5000
        # quota, reset_at = await fetch_quota(client_post, print_results=True)
        rate_limit = AsyncLimiter(quota, 3600) # 5000 hits in 3600 secs max

        for payload in ctx:
            for _ in range(REPO_QUERY_RETRIES):
                async with rate_limit:
                    resp = await client_post(json = payload)
                resp_dict = resp.json()
                if "errors" in resp_dict or "message" in resp_dict:
                    print(resp_dict)
                    continue
                break
            else:
                print("Possible rate limit reached. "
                    f"Waiting for {REPO_QUERY_RATELIMIT_WAIT_SECS} seconds")
                await anyio.sleep(REPO_QUERY_RATELIMIT_WAIT_SECS)
                ctx.page = ctx.PageEnum.CONT
                continue

            page = QueryPage.from_dict(resp_dict)

            yield page
            ctx.page = page
            ctx.show_progress(page, payload["variables"])

async def fetch_fill_repo_db():
    with repo_raw_table.connect_database():
        if REPO_TABLE_DROP_BEFORE_RUN:
            repo_raw_table.drop()
        repo_raw_table.create()
        async for page in fetch_pages_aiter():
            repo_raw_table.add_rows(repo.to_tuple() for repo in page.repos)

class _LoopContext:
    class PageEnum(Enum):
        NO_PAGE = 0
        CONT = 1

    def __init__(self):
        self.page: Union[QueryPage, self.PageEnum] = self.PageEnum.NO_PAGE
        self.page_num: int = 1
        self.last_page: Optional[QueryPage] = None
        self.time: float = 0
        self.default_gql_payload = {
            "query": Path(REPO_QUERY_GRAPHQL_PATH).read_text(),
            "variables": REPO_QUERY_GRAPHQL_PARAMS,
        }
        self.ranged_params = dict(REPO_QUERY_RANGE_PARAMS)
        self._make_comparable_ranged_params()

    def __iter__(self):
        return self

    def _make_comparable_ranged_params(self):
        self.ranged_params["min_created"] = dateutil_parse(self.ranged_params["min_created"])
        self.ranged_params["max_created"] = dateutil_parse(self.ranged_params["max_created"])

    def __next__(self):
        page = self.page
        if page is self.PageEnum.NO_PAGE:
            # first run: like do-while
            self.time = get_current_time()
            return self.new_payload()

        if page is self.PageEnum.CONT:
            return self.new_payload()

        if isinstance(page, QueryPage):
            """
            Update stuffs after an iteration
            """
            if self.ranged_params["min_created"] <= datetime.now(timezone.utc):
                self.page_num += 1
                self.last_page = self.page
                return self.new_payload()

            raise StopIteration

        raise TypeError(f"Page must be of type {self.PageEnum} or {QueryPage}")

    def new_payload(self):
        payload = deepcopy(self.default_gql_payload)
        variables = payload["variables"]
        self._modify_params(variables)
        extended_query_str = self._ranged_params_to_query(self.ranged_params)
        variables["queryStr"] = self._join_query_strings(variables["queryStr"], extended_query_str)
        return payload

    def _modify_params(self, variables: Optional[dict] = None):
        INCREASE_DAY_BY = 2
        page = self.page

        if variables is None:
            variables = {}

        if isinstance(page, QueryPage):
            if not page.has_next_page or page.has_no_results():
                try:
                    del variables["lastCursorId"]
                except KeyError:
                    pass
                # see https://dateutil.readthedocs.io/en/stable/relativedelta.html
                delta = dateutil_delta(days=INCREASE_DAY_BY)
                self.ranged_params["min_created"] += delta
                self.ranged_params["max_created"] += delta
            else:
                variables["lastCursorId"] = page.last_cursor_id

    def _ranged_params_to_query(self, ranged_params: dict):
        # remove min_ or max_
        # get prop names
        props = set(prop[4:] for prop in ranged_params)

        key_vals = []
        for prop in props:
            min_prop, max_prop = f"min_{prop}", f"max_{prop}"
            has_min, has_max = (min_prop in ranged_params), (max_prop in ranged_params)
            if has_min and has_max:
                _min, _max = ranged_params[min_prop], ranged_params[max_prop]
                _min, _max = min(_min, _max), max(_min, _max)
                _min, _max = self.param_to_str(_min), self.param_to_str(_max)
                key_val = f"{prop}:{_min}..{_max}"
            elif has_min:
                _min = self.param_to_str(ranged_params[min_prop])
                key_val = f"{prop}:>={_min}"
            elif has_max:
                _max = self.param_to_str(ranged_params[max_prop])
                key_val = f"{prop}:<={_max}"
            else:
                raise Exception(f"Expected min_{prop} or max_{prop} in ranged_params")
            key_vals.append(key_val)

        _str = " ".join(key_vals)
        return _str

    def param_to_str(self, param, datetime_format = "%Y-%m-%dT%H:%M:%SZ"):
        if isinstance(param, datetime):
            return param.strftime(datetime_format)
        return str(param)

    def _join_query_strings(self, *query_strings: str):
        # join "a:b c:d", "a:h e:f" into "a:h c:d e:f"
        memo = {}
        for _str in query_strings:
            for key_val in _str.strip().split():
                key, val = key_val.split(":", maxsplit=1)
                memo[key] = val

        delimiter = " "
        final_query_str = delimiter.join(f"{key}:{val}" for key, val in memo.items())
        return final_query_str

    def get_elapsed(self):
        return str(timedelta(seconds=get_current_time() - self.time))

    def show_progress(self, page: QueryPage, variables: dict):
        def ljust(obj, max_obj):
            return str(obj).ljust(len(str(max_obj)))

        page_num = str(self.page_num).ljust(6)
        has_next_page = "Y" if page.has_next_page else "N"

        MAX_QUOTA = 5000
        MAX_NODES = 100
        MAX_CURSOR = "Y3Vyc29yOjEwNw=="
        MAX_PAGE_NUM = 9999999

        datetime_format = "%Y-%m-%d"
        min_created = self.param_to_str(self.ranged_params["min_created"], datetime_format)
        max_created = self.param_to_str(self.ranged_params["max_created"], datetime_format)

        key_vals = (
            f"Page {ljust(page_num, MAX_PAGE_NUM)}",
            f"Quota {ljust(page.quota, MAX_QUOTA)}",
            f"Nodes {ljust(len(page.repos), MAX_NODES)}",
            f"NextPage {has_next_page}",
            f"Cursor {ljust(page.last_cursor_id, MAX_CURSOR)}",
            f"{min_created} to {max_created}",
            f"Elapsed {self.get_elapsed()}"
        )

        delimiter = " | "
        print(delimiter.join(key_vals))

if __name__ == "__main__":
    if check_config_prompt():
        anyio.run(fetch_fill_repo_db)
