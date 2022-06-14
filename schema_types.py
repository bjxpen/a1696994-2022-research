from typing import List, Optional, Iterable, Any

import dataclasses

REPO_TABLE_SCHEMA = """
    id INTEGER PRIMARY KEY
    owner TEXT
    name TEXT 
    stars INTEGER
    is_fork INTEGER
    kilobytes INTEGER
    created_at TEXT
    updated_at TEXT
    description TEXT
    closed_issue_count INTEGER
    commit_count INTEGER
    topics TEXT
    readme TEXT
"""

def transform_values(values: Iterable[Any], 
    fields: Iterable[dataclasses.Field]
):    
    for value, field in zip(values, fields):
        if value is None:
            if field.type in (int, float):
                value = 0
            elif field.type is str:
                value = ""
            elif field.type is bool:
                value = False
        if not isinstance(value, field.type):
            value = field.type(value)
        yield value

@dataclasses.dataclass(frozen=True)
class RepositoryBase:
    id: int
    owner: str
    name: str
    stars: int
    is_fork: bool
    kilobytes: int
    created_at: str
    updated_at: str
    description: str
    closed_issue_count: int
    commit_count: int
    topics: str = ""
    readme: str = ""

    def get_name_with_owner(self):
        return f"{self.owner}/{self.name}"

    def get_url(self, prefix = "https://github.com/"):
        import urllib.parse
        return urllib.parse.urljoin(prefix, self.get_name_with_owner())

RepositoryFields = dataclasses.fields(RepositoryBase)

class Repository(RepositoryBase):
    def __init__(self, *args):
        self._tuple_values = args
        super().__init__(*args)
        
    def to_tuple(self):
        # differernt from as tuple
        return self._tuple_values
        
    @classmethod
    def from_dict(cls, dic):
        owner, name = dic["nameWithOwner"].split("/")
        
        # topics in insertion order by repo authors
        topics = [topic_node["topic"]["name"] for topic_node in dic["topics"]["nodes"]]
        topics_str = "|".join(topics)
        
        commit_count = dic["defaultBranchRef"]["target"]["history"]["totalCount"]
        readme = dic.get("readmeCaps", dic.get("readmeLowercase", ""))

        repo = Repository(
            dic["id"],
            owner,
            name,
            dic["stars"],
            dic["isFork"],
            dic["kilobytes"],
            dic["createdAt"],
            dic["updatedAt"],
            dic["description"],
            dic["closedIssues"]["totalCount"],
            commit_count,
            topics_str,
            readme,
        )
            
        return repo

    @classmethod
    def from_tuple(cls, tuple):
        new_tuple = transform_values(tuple, RepositoryFields)
        return Repository(*new_tuple)


@dataclasses.dataclass(frozen=True)
class QueryPage:
    total_repos: int
    last_cursor_id: str
    has_next_page: bool
    repos: List[Repository] = dataclasses.field(default_factory=list)
    cost: Optional[int] = 0
    quota: Optional[int] = 0
    
    @classmethod
    def from_dict(self, dic):
        dic = dic["data"]
        search_dic = dic["search"]
        usage_dic = dic["rateLimit"]
        
        repos = list(Repository.from_dict(repo_dict) for repo_dict in search_dic["nodes"])
        
        page = QueryPage(
            search_dic["repositoryCount"],
            search_dic["pageInfo"]["lastCursorId"],
            search_dic["pageInfo"]["hasNextPage"],
            repos = repos,
            cost = usage_dic["cost"],
            quota = usage_dic["remaining"],
        )
        
        return page

    def get_min_repo_stars(self):
        return sorted(self.repos, key = lambda repo: repo.stars)[0].stars

    def get_latest_created_at(self):
        return sorted(self.repos, key = lambda repo: repo.created_at)[-1].created_at
    
    def get_latest_updated_at(self):
        return sorted(self.repos, key = lambda repo: repo.updated_at)[-1].updated_at

    def has_no_results(self):
        return len(self.repos) == 0
