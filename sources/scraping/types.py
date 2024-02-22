import typing as t

AnyDict = t.Dict[str, t.Any]

# start_urls can be a file path or a list of urls
StartUrls = t.Optional[t.Union[t.List[str], str]]
P = t.ParamSpec("P")


class Runnable(t.Protocol):
    def run(self, *args: P.args, **kwargs: P.kwargs) -> t.Any:
        pass
