from typing import List, Dict, Any, Union
from fsspec.registry import register_implementation
from fsspec.spec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile
import git


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

    An instance of this class provides the files residing within a remote github
    repository. You may specify a point in the repo's history, by SHA, branch
    or tag (default is current master).

    You can retrieve information such as a file's modified time, which would not
    be possible if looking at the local filesystem directly.

    It is based on the gitpython library, which could be used to clone or update
    files from a remote repo before reading them with this filesystem.
    """

    protocol = "gitpythonfs"

    def __init__(self, path: str, ref: str = None, **kwargs: Any) -> None:
        """
        Initialize a GitPythonFS object.

        Args:
            path (str): Local location of the Git repo. When used with a higher
                level function such as fsspec.open(), may be of the form
                "gitpythonfs://[path-to-repo:][ref@]path/to/file" so that repo
                and/or ref can be passed in the URL instead of arguments. (The
                actual file path should not contain "@" or ":"). Examples:
                    When instantiating GitPythonFileSystem:
                    /some_folder/my_repo
                    When calling open(), open_files() etc:
                    gitpythonfs:///some_folder/my_repo:path/to/intro.md
                    gitpythonfs:///some_folder/my_repo:mybranch@path/to/intro.md
            ref (str): (To be implemented). A branch, tag or commit hash to use.
                Defaults to head of the local repo.
        """
        super().__init__(**kwargs)
        self.repo_path = path
        self.repo = git.Repo(self.repo_path)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        path = super()._strip_protocol(path).lstrip("/")
        if ":" in path:
            path = path.split(":", 1)[1]
        if "@" in path:
            path = path.split("@", 1)[1]
        return path.lstrip("/")

    # ToDo support arguments in url, like this example from git fsspec implementation:
    @staticmethod
    def _get_kwargs_from_urls(path: str) -> Dict[str, str]:
        if path.startswith("gitpythonfs://"):
            path = path[14:]
        out = {}
        if ":" in path:
            out["path"], path = path.split(":", 1)
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
        self, object: git.Object, include_committed_date: bool = True
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
        # commit=next(self.repo.iter_commits(paths=object.path, max_count=1))
        details = {
            "name": object.path,
            "type": self._git_type_to_file_type(object),
            "mime_type": object.mime_type if isinstance(object, git.Blob) else None,
            "size": object.size,
            "hexsha": object.hexsha,
            # "committed_date": commit.committed_date,
        }

        if include_committed_date:
            commit = next(self.repo.iter_commits(paths=object.path, max_count=1))
            details["committed_date"] = commit.committed_date

        return details

    def ls(
        self, path: str, detail: bool = False, ref: str = None, **kwargs: Any
    ) -> Union[List[str], List[Dict]]:  # Todo implement ref
        """List files at given path in the repo."""
        path = self._strip_protocol(path)
        results = []

        # For traversal, always start at the root of repo.
        tree = self.repo.tree()
        root_object = tree if path == "" else tree / path

        if isinstance(root_object, git.Tree):
            if detail:
                for object in root_object:
                    results.append(self._details(object, **kwargs))
                return results
            else:
                for object in root_object:
                    results.append(object.path)
                return results
        else:
            # path is to a single blob.
            if detail:
                results.append(self._details(root_object, **kwargs))
                return results
            else:
                results.append(root_object.path)
                return results

    # ToDo implement refs
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
        # ToDo: support refs, with something like `ref or self.ref`.
        path = self._strip_protocol(path)
        tree = self.repo.tree()
        blob = tree / path
        return MemoryFile(data=blob.data_stream.read())

    READ_ONLY_MESSAGE = "This fsspec implementation is read-only."

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
