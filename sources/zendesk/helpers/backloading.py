from typing import Dict, Any

from dlt.common import pendulum

from dlt.common.configuration.specs import BaseConfiguration, configspec


@configspec
class SliceConfig(BaseConfiguration):
    start: pendulum.DateTime = None
    end: pendulum.DateTime = None
    cursor_path: str = None

    def __init__(
        self, start: pendulum.DateTime, end: pendulum.DateTime, cursor_path: str
    ) -> None:
        self.start = start
        self.end = end
        self.cursor_path = cursor_path

    def to_endpoint_kwargs(self) -> Dict[str, Any]:
        return dict(
            start_time=self.start, until=self.end, timestamp_field=self.cursor_path
        )
