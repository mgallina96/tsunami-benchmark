from datetime import datetime
from typing import Callable, Awaitable
from uuid import uuid4

import aiohttp
from aiohttp import ClientTimeout


def log(message: str):
    print(f"[{datetime.now()}] {message}")


class TsunamiConfiguration:
    """Base class for all TsunamiConfiguration classes."""

    task: Callable[[], Awaitable[bool]]
    """The task to be executed."""
    weight: float
    """The weight of the task. Used to calculate the probability of the task being executed."""

    def __init__(self, task: Callable[[], Awaitable[bool]], *, weight: float = 1):
        self.task = task
        self.weight = weight


class GraphqlPostConfiguration(TsunamiConfiguration):
    """Configuration for a graphql post request."""

    session: aiohttp.ClientSession
    """AIOHttp client session to be used for the request."""
    url: str
    """URL of the graphql ."""
    headers: dict | None
    """Headers to be used for the request."""
    body: str
    """Body containing the GraphQL query to send."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        url: str,
        body: str,
        *,
        headers: dict | None = None,
        weight: float = 1,
    ):
        self.session = session
        self.url = url
        self.body = body
        self.headers = headers
        super().__init__(self.send_request, weight=weight)

    async def send_request(self) -> bool:
        try:
            uuid = str(uuid4())
            log(f"Request: uuid={uuid}")
            async with self.session.post(
                url=self.url,
                json={"query": self.body},
                headers=self.headers,
                timeout=ClientTimeout(total=600),
            ) as response:
                response_body = await response.read()
                log(
                    f"Response: uuid: {uuid}, status={response.status}, body={response_body.decode('utf-8')}"
                )
                return response.ok
        except Exception as e:
            log(f"Error due to {e.__class__}.")
            return False
