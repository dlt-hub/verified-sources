import re
from git import Repo, Diff

pipelines = set()
with Repo(".", search_parent_directories=True) as repo:
    print(repo.active_branch.name)
    diff: Diff
    # for diff in repo.head.commit.diff("master", create_patch=False):
    #     rm: re.Match
    #     if rm := re.match("^pipelines[\/](.+)[\/]", diff.a_path):
    #         pipelines.add(rm.group(1))