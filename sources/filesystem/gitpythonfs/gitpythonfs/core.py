from typing import List, Dict, Any, Union
from fsspec.registry import register_implementation
from fsspec.spec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile
import git
from functools import lru_cache
from gitpythonfs import git_cmd


def register_implementation_in_fsspec() -> None:
    """Dyanmically register the filesystem with fsspec.

    This is needed if the implementation is not officially registered in the fsspec codebase.
    It will also override ("clobber") an existing implementation having the same protocol.
    The registration is only valid for the current process.
    """
    register_implementation(
        "gitpythonfs",
        "gitpythonfs.GitPythonFileSystem",
        clobber=True,
        errtxt="Please install gitpythonfs to access GitPythonFileSystem",
    )


class GitPythonFileSystem(AbstractFileSystem):
    """A filesystem for git repositories on the local filesystem.

    An instance of this class provides the files residing within a local git
    repository. You may specify a point in the repo's history, by SHA, branch
    or tag (default is HEAD).

    You can retrieve information such as a file's modified time, which would not
    be possible if looking at the local filesystem directly.

    It is based on the gitpython library, which could also be used to clone or update
    files from a remote repo before reading them with this filesystem.

    Instances of this class cache some git objects so it not recommended to change
    the git repo within the lifetime of an instance. Calling clear_git_caches() may help.
    Also note that fsspec itself caches entire instances, which can be overridden with
    the `skip_instance_cache=True` keyword argument.
    """

    PROTOCOL = "gitpythonfs"
    READ_ONLY_MESSAGE = "This fsspec implementation is read-only."

    def __init__(self, repo_path: str = None, ref: str = None, **kwargs: Any) -> None:
        """
        Initialize a GitPythonFS object.

        Args:
            repo_path (str): Local location of the Git repo. Defaults to current directory.

                When used with a higher
                level function such as fsspec.open(), may be of the form
                "gitpythonfs://[path-to-repo:][ref@]path/to/file" so that repo
                and/or ref can be passed in the URL instead of arguments. (The
                actual file path should not contain "@" or ":"). Examples:
                    When instantiating GitPythonFileSystem:
                    /some_folder/my_repo
                    When calling open(), open_files() etc:
                    gitpythonfs:///some_folder/my_repo:path/to/intro.md
                    gitpythonfs:///some_folder/my_repo:mybranch@path/to/intro.md
            ref (str): A branch, tag or commit hash to use.
                Defaults to HEAD of the local repo.
        """
        super().__init__(**kwargs)
        self.repo_path = repo_path
        self.repo = git.Repo(self.repo_path)
        self.ref = ref or self.repo.head.ref.name

        self._get_tree = lru_cache(maxsize=128)(self._get_tree_uncached)

    def _get_tree_uncached(self, ref: str) -> git.Tree:
        """Get the tree at repo root for a given ref

        Args:
            ref (str): The reference to the commit, branch or tag

        Returns:
            git.Tree: The tree object at the root of the repository for the given ref
        """
        return self.repo.tree(ref)

    def clear_git_caches(self) -> None:
        """Clear the git caches.

        This is useful if the repo has changed and you want to force a refresh of the
        cached objects. Also not that fsspec itself may cache instances of AbstractFileSystem.
        """
        self._get_tree.cache_clear()

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        path = super()._strip_protocol(path).lstrip("/")
        if ":" in path:
            path = path.split(":", 1)[1]
        if "@" in path:
            path = path.split("@", 1)[1]
        return path.lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> Dict[str, str]:
        if path.startswith("gitpythonfs://"):
            path = path[14:]
        out = {}
        if ":" in path:
            out["repo_path"], path = path.split(":", 1)
        if "@" in path:
            out["ref"], path = path.split("@", 1)
        return out

    def _git_type_to_file_type(self, object: git.Object) -> str:
        if isinstance(object, git.Blob):
            return "file"
        elif isinstance(object, git.Tree):
            return "directory"
        else:
            msg = f"There is no fileystem object type corresponding to Git object type: {type(object).__name__}"
            raise TypeError(msg)

    def _details(
        self,
        object: git.Object,
        details_by_path: dict = None,
        include_committed_date: bool = True,
    ) -> Dict[str, Union[str, int]]:
        """
        Retrieves the details of a Git object.

        Args:
            object (git.Object): The Git object to retrieve details for.
            include_committed_date (bool, optional): Whether to include the committed date. Defaults to True.
                Getting the committed date is an expensive operation and will slow down
                walk(), a method that is extensively used by fsspec for find(), glob() etc.

        Returns:
            dict: A dictionary containing the details typical for fsspec.
        """
        details = {
            "name": object.path,
            "type": self._git_type_to_file_type(object),
            "mode": f"{object.mode:o}",
            "mime_type": object.mime_type if isinstance(object, git.Blob) else None,
            "size": object.size,
            "hex": object.hexsha,
        }

        if isinstance(object, git.Blob) and include_committed_date:
            details["committed_date"] = details_by_path[object.path]

        return details

    def ls(
        self, path: str, detail: bool = False, ref: str = None, **kwargs: Any
    ) -> Union[List[str], List[Dict]]:
        """List files at given path in the repo."""
        path = self._strip_protocol(path)
        include_committed_date = kwargs.get("include_committed_date", True)
        details_by_path = {}
        results = []

        # GitPython recommends always starting at root of repo.
        tree = self._get_tree(ref or self.ref)

        object_at_path = tree if path == "" else tree / path
        object_type = self._git_type_to_file_type(object_at_path)

        # Details that are faster to get in bulk via git command.
        if detail and include_committed_date:
            raw_details = git_cmd.get_revisions_info(
                self.repo, ref or self.ref, path, object_type
            )
            details_by_path = git_cmd.parse_git_revlist(raw_details)

        if isinstance(object_at_path, git.Tree):
            if detail:
                for object in object_at_path:
                    results.append(
                        self._details(object, details_by_path=details_by_path, **kwargs)
                    )
                return results
            else:
                for object in object_at_path:
                    results.append(object.path)
                return results
        else:
            # path is to a single blob.
            if detail:
                results.append(
                    self._details(
                        object_at_path, details_by_path=details_by_path, **kwargs
                    )
                )
                return results
            else:
                results.append(object_at_path.path)
                return results

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options=None,
        ref: str = None,
        **kwargs: Any,
    ) -> MemoryFile:
        path = self._strip_protocol(path)
        tree = self._get_tree(ref or self.ref)
        blob = tree / path
        return MemoryFile(data=blob.data_stream.read())

    def mv(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def rm(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def touch(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def mkdir(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def mkdirs(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def rmdir(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def put_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def put(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def cp_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def copy(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def rm_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def _rm(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def chmod(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)

    def chown(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(self.READ_ONLY_MESSAGE)
