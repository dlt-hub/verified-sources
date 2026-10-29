"""Connection to a Salesforce org shared by the query APIs"""

import threading
from typing import Any, Callable, Dict, Literal, Optional

from dlt.common import logger
from dlt.sources.helpers.requests import Response
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceExpiredSession
from simple_salesforce.util import call_salesforce

# `instance` takes paths that start at the instance root, e.g. REST API next page urls
TUrlBase = Literal["bulk2", "rest", "instance"]


class SalesforceConnection:
    """Sends requests to a Salesforce org, errors are raised as `simple_salesforce` exceptions.

    Logs in with `login` on first use and when the session expires. Instances may be shared by
    threads.
    """

    def __init__(
        self, login: Callable[[], Salesforce], client: Optional[Salesforce] = None
    ) -> None:
        self.login = login
        self._client = client
        self._login_lock = threading.Lock()

    @property
    def client(self) -> Salesforce:
        """Logged in client"""
        if self._client is None:
            with self._login_lock:
                if self._client is None:
                    self._client = self.login()
        return self._client

    @client.setter
    def client(self, client: Salesforce) -> None:
        self._client = client

    @property
    def instance_url(self) -> str:
        return f"https://{self.client.sf_instance}"

    def describe(self, sobject: str) -> Dict[str, Any]:
        """Returns the `describe()` result of `sobject`"""
        return self.request("GET", "rest", f"sobjects/{sobject}/describe").json()  # type: ignore[no-any-return]

    def request(
        self,
        method: str,
        base: TUrlBase,
        path: str,
        accept: str = "application/json",
        **kwargs: Any,
    ) -> Response:
        """Sends a request to `path` under the `base` url, logging in again if the session expired"""
        client = self.client
        try:
            return self._call(client, method, base, path, accept, **kwargs)
        except SalesforceExpiredSession:
            # a long job may outlive the session. job ids, result locators and REST cursors stay
            # valid in the new session so the call is repeated
            with self._login_lock:
                # threads sharing this instance log in once, others use the new session
                if self._client is client:
                    logger.info("Salesforce session expired, logging in again")
                    self._client = self.login()
            return self._call(self.client, method, base, path, accept, **kwargs)

    @staticmethod
    def _call(
        client: Salesforce,
        method: str,
        base: TUrlBase,
        path: str,
        accept: str,
        headers: Dict[str, str] = None,
        **kwargs: Any,
    ) -> Response:
        base_url = {
            "bulk2": client.bulk2_url,
            "rest": client.base_url,
            "instance": f"https://{client.sf_instance}",
        }[base]
        return call_salesforce(
            url=base_url + path,
            method=method,
            session=client.session,
            headers={
                "Authorization": "Bearer " + client.session_id,
                "Content-Type": "application/json",
                "Accept": accept,
                **(headers or {}),
            },
            **kwargs,
        )


def is_concurrent_limit_exceeded(
    response: Optional[Response], exception: Optional[BaseException]
) -> bool:
    """Tells if Salesforce refused a request because the org runs too many long requests"""
    # the daily request limit has the same error code and is not retried
    return (
        response is not None
        and response.status_code == 403
        and "ConcurrentPerOrgLongTxn" in response.text
    )
