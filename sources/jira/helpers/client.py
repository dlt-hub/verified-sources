"""Client for the REST API of a Jira Cloud site"""

from typing import Any, List, Optional

from dlt.common import logger
from dlt.common.typing import DictStrAny
from dlt.sources.helpers.requests import Client, HTTPError, Response

from ..settings import MYSELF_PATH

HEADERS = {"Accept": "application/json"}


class JiraClient:
    """Calls the REST API of a Jira Cloud site and retries on HTTP 429 and 5xx.

    Sessions are thread local. Errors are raised with the messages returned by Jira, e.g. for an
    invalid JQL query.
    """

    def __init__(self, subdomain: str, email: str, api_token: str) -> None:
        self.base_url = f"https://{subdomain}.atlassian.net/"
        self._client = Client(
            raise_for_status=False, session_attrs={"auth": (email, api_token)}
        )

    def get_json(self, path: str, params: Optional[DictStrAny] = None) -> Any:
        response = self._client.session.get(
            self.base_url + path, params=params, headers=HEADERS
        )
        return self._json(response)

    def post_json(self, path: str, body: DictStrAny) -> Any:
        response = self._client.session.post(
            self.base_url + path, json=body, headers=HEADERS
        )
        return self._json(response)

    def get_time_zone(self) -> str:
        """Returns the time zone of the user, JQL interprets datetime literals in it"""
        time_zone: Optional[str] = self.get_json(MYSELF_PATH).get("timeZone")
        if not time_zone:
            logger.warning(
                "Jira user has no time zone, JQL datetimes are rendered in UTC"
            )
            return "UTC"
        return time_zone

    @staticmethod
    def _json(response: Response) -> Any:
        if response.status_code >= 400:
            messages: List[str] = []
            try:
                error = response.json()
                messages.extend(error.get("errorMessages") or [])
                messages.extend(
                    f"{k}: {v}" for k, v in (error.get("errors") or {}).items()
                )
            except (ValueError, AttributeError):
                pass
            raise HTTPError(
                f"{response.status_code} {response.reason} for url: {response.url}"
                + (f" Jira errors: {'; '.join(messages)}" if messages else ""),
                response=response,
            )
        return response.json()
