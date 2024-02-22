import typing as t

AnyDict = t.Dict[str, t.Any]

P = t.ParamSpec("P")


class Runnable(t.Protocol):
    def run(self, *args: P.args, **kwargs: P.kwargs) -> t.Any:
        pass
