#!/usr/bin/env python3

"""Script to verify that all sources have a requirements.txt
file containing a versioned dlt requirement"""
from typing import Dict

from sys import exit
from pathlib import Path
from packaging.requirements import Requirement, InvalidRequirement

sources_path = Path("sources")

source_dirs = [
    x
    for x in sources_path.iterdir()
    if x.is_dir() and not x.name.startswith((".", "_"))
]

error = False

error_msg_suffix = (
    "Please add a requirements.txt file with a versioned dlt requirement without extras. "
    "E.g. dlt>=0.3.5,<0.4.0"
)


def has_url_with_pin(dlt_req: Requirement) -> bool:
    """Checks if the url contains a reference to a branch, tag, or commit"""
    return dlt_req.url is not None and "@" in dlt_req.url


for source in source_dirs:
    req_path = source.joinpath("requirements.txt")
    if not req_path.is_file():
        print(
            f"ERROR: Source {source.name} has no requirements.txt file. {error_msg_suffix}"
        )
        error = True
        continue
    req_text = req_path.read_text(encoding="utf-8")
    req_lines = req_text.splitlines()
    parsed_reqs: Dict[str, Requirement] = {}
    for req_str in req_lines:
        try:
            req = Requirement(req_str)
        except InvalidRequirement:
            print(f"ERROR: Source {source.name} has invalid requirement '{req_str}'")
            raise
        else:
            parsed_reqs[req.name] = req

    if "dlt" not in parsed_reqs:
        print(f"ERROR: Source {source.name} has no dlt requirement. {error_msg_suffix}")
        error = True
        continue
    dlt_req = parsed_reqs["dlt"]
    if dlt_req.extras:
        print(
            f"ERROR: Source {source.name} dlt requirement '{dlt_req}' contains extras. {error_msg_suffix}"
        )
        error = True
        continue
    if not dlt_req.specifier and not has_url_with_pin(dlt_req):
        print(
            f"ERROR: Source {source.name} dlt requirement '{dlt_req}' has no version constraint. {error_msg_suffix}"
        )
        error = True
        continue

if error:
    exit(1)
