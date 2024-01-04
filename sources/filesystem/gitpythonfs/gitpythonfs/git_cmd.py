from typing import Dict
import git


def get_revisions_info(repo: git.Repo, ref: str, path: str, object_type: str) -> str:
    """
    Get the git revisions at a given ref and path. Does not recurse into subdirectories.

    Args:
        repo (git.Repo): The git repository object.
        ref (str): The reference (commit, branch, tag, etc.) to get the revisions from.
        path (str): The path to the file or directory to get the revisions for.
            The path is from the root of the repo.  The path should exist at,
            the given ref, but does not need to exist in the local file system.
            For the root folder, use an empty string. ToDo: check this behaviour.
        object_type (str): The type of the object at the given path.  Either "file" or "directory".

    Returns:
        str: The revisions at the given ref and path, as would be on stdout".
    """

    # repo = git.Repo("/tmp/repo_fixture")
    git_cmd_runner = repo.git

    # todo: use pathlib?
    if path and path[0] in "\\/":
        raise ValueError("Path should not start with a slash")

    if object_type == "file" and not path:
        raise ValueError("Invalid path. For a file, path should be the filename.")

    # git uses fnmatch(3) style matching
    path_spec = ":(top,glob)" + path
    if object_type == "directory":
        if path:
            # get objects in directory.
            path_spec += "/*"
        else:
            # get objects in root. fnmatch does not like leading slash.
            path_spec += "*"

    out = git_cmd_runner.log(ref, path_spec, raw=True, no_merges=True, pretty="%at")
    return out


def parse_git_revlist(git_cmd_output: str) -> Dict[str, int]:
    """
    Parse raw text output produced by git rev-list

    `git log` and `git whatchanged` have similar output.

    Args:
        raw_text (str): The git cmd output to parse.

    Returns:
        dict: The parsed revisions info as a dictionary with:
            key: path to file
            value: most recent commit timestamp for file
    """
    revisions_info = {}
    done_files = []

    lines = git_cmd_output.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if not line.startswith(":"):
            # it's the line with the pretty format info
            committed_at = int(line)
        else:
            filepath = line.split("\t")[-1]
            # first occurence of a file is the most recent.
            if not filepath in done_files:
                revisions_info[filepath] = committed_at
                done_files.append(filepath)

    return revisions_info
