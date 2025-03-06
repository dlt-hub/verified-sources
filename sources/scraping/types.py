import typing as t

AnyDict = t.Dict[str, t.Any]


class Runnable(t.Protocol):
    def run(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        pass
