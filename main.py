from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class AnimalData:
    id: str
    name: str
    friends: List[str]
    born_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
class AnimalAPIClient:

    def __init__(self, base_url: str = "http://localhost:3123"):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        if not self.session:
            raise RuntimeError("Client session not initialized")

        url = f"{self.base_url}{endpoint}"

        try:
            async with self.session.request(method, url, **kwargs) as response:
                if response.status in [500, 502, 503, 504]:
                    response.raise_for_status()

                response.raise_for_status()
                return await response.json()

        except aiohttp.ClientError as e:
            raise