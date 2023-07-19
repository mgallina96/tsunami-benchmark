import asyncio

import aiohttp
from aiohttp import ClientTimeout

from models.configuration import TsunamiTask


class DemoTask(TsunamiTask):
    """Demo task."""

    text: str
    """Text to be printed."""
    wait: float
    """Time to wait before printing the goodbye."""

    def __init__(self, text: str, *, wait: float = 0):
        self.text = text
        self.wait = wait

    async def run(self) -> tuple[bool, dict]:
        print(f"Hello {self.text}")
        await asyncio.sleep(self.wait)
        print(f"Goodbye {self.text}")
        return True, {"text": self.text}


class GraphqlPostTask(TsunamiTask):
    """Task for a graphql post request."""

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
    ):
        self.session = session
        self.url = url
        self.body = body
        self.headers = headers

    async def run(self) -> tuple[bool, dict]:
        try:
            async with self.session.post(
                url=self.url,
                json={"query": self.body},
                headers=self.headers,
                timeout=ClientTimeout(total=600),
            ) as response:
                response_body = await response.read()
                return response.ok, {"status": response.status, "body": response_body}
        except Exception as e:
            return False, {"error": str(e)}
