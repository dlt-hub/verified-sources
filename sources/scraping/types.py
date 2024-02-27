import typing as t

from typing_extensions import ParamSpec

AnyDict = t.Dict[str, t.Any]

P = ParamSpec("P")


class Runnable(t.Protocol):
    def run(self, *args: P.args, **kwargs: P.kwargs) -> t.Any:
        pass
