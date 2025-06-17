import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict
from dateutil.parser import parse as parse_date
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        logger.debug(f"Making {method} request to {url}")

        try:
            async with self.session.request(method, url, **kwargs) as response:
                if response.status in [500, 502, 503, 504]:
                    logger.warning(f"Server error {response.status}, retrying...")
                    response.raise_for_status()

                response.raise_for_status()
                return await response.json()

        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {e}")
            raise

    async def get_animals_page(self, page: int = 1) -> Dict[str, Any]:
        return await self._make_request(
            "GET",
            "/animals/v1/animals",
            params={"page": page}
        )

    async def get_animal_details(self, animal_id: str) -> Dict[str, Any]:
        return await self._make_request(
            "GET",
            f"/animals/v1/animals/{animal_id}"
        )

    async def post_animals_to_home(self, animals: List[Dict[str, Any]]) -> Dict[str, Any]:
        if len(animals) > 100:
            raise ValueError("Cannot send more than 100 animals at once")

        return await self._make_request(
            "POST",
            "/animals/v1/home",
            json=animals
        )


class AnimalDataTransformer:

    @staticmethod
    def transform_friends(friends_str: Optional[str]) -> List[str]:
        if not friends_str or friends_str.strip() == "":
            return []
        return [friend.strip() for friend in friends_str.split(",") if friend.strip()]

    @staticmethod
    def transform_born_at(born_at_input: Optional[Any]) -> Optional[str]:
        if born_at_input is None:
            return None

        if isinstance(born_at_input, (int, float)):
            try:
                timestamp_seconds = born_at_input / 1000
                parsed_date = datetime.fromtimestamp(timestamp_seconds)
                return parsed_date.isoformat() + "Z"
            except (ValueError, OSError) as e:
                logger.warning(f"Failed to parse timestamp '{born_at_input}': {e}")
                return None

        if isinstance(born_at_input, str):
            if not born_at_input.strip():
                return None
            try:
                parsed_date = parse_date(born_at_input)
                return parsed_date.isoformat()
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse date '{born_at_input}': {e}")
                return None

        logger.warning(f"Unknown born_at format: {type(born_at_input)} - {born_at_input}")
        return None

    @classmethod
    def transform_animal_data(cls, raw_data: Dict[str, Any]) -> AnimalData:
        return AnimalData(
            id=str(raw_data.get("id", "")),
            name=raw_data.get("name", ""),
            friends=cls.transform_friends(raw_data.get("friends")),
            born_at=cls.transform_born_at(raw_data.get("born_at")),
        )


class AnimalETLPipeline:

    def __init__(self, api_client: AnimalAPIClient, batch_size: int = 100):
        self.api_client = api_client
        self.batch_size = batch_size
        self.transformer = AnimalDataTransformer()
        self.processed_count = 0

    async def extract_all_animal_ids(self) -> List[str]:
        animal_ids = []
        page = 1

        logger.info("Starting animal ID extraction...")

        while True:
            try:
                logger.debug(f"Fetching page {page}")
                response = await self.api_client.get_animals_page(page)
                logger.debug(f"Page {page} response: {response}")

                animals = response.get("items", [])

                if not animals:
                    logger.info(f"No animals found on page {page}, stopping pagination")
                    break

                page_ids = [str(animal.get("id")) for animal in animals if animal.get("id") is not None]
                animal_ids.extend(page_ids)

                logger.info(f"Extracted {len(page_ids)} IDs from page {page}")

                page += 1

                if page > 1000:
                    logger.warning("Reached maximum page limit (1000), stopping")
                    break

            except Exception as e:
                logger.error(f"Failed to extract page {page}: {e}")
                if page == 1:
                    logger.error("First page failed, cannot continue")
                    break
                else:
                    logger.warning(f"Page {page} failed, assuming end of data")
                    break

        logger.info(f"Total animal IDs extracted: {len(animal_ids)}")
        return animal_ids

    async def extract_and_transform_animal(self, animal_id: str) -> Optional[AnimalData]:
        try:
            raw_data = await self.api_client.get_animal_details(animal_id)
            return self.transformer.transform_animal_data(raw_data)
        except Exception as e:
            logger.error(f"Failed to process animal {animal_id}: {e}")
            return None

    async def process_animals_batch(self, animal_ids: List[str]) -> List[AnimalData]:
        tasks = [
            self.extract_and_transform_animal(animal_id)
            for animal_id in animal_ids
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_animals = [
            animal for animal in results
            if isinstance(animal, AnimalData)
        ]

        return valid_animals

    async def load_animals_batch(self, animals: List[AnimalData]) -> bool:
        if not animals:
            return True

        try:
            animal_dicts = [animal.to_dict() for animal in animals]
            await self.api_client.post_animals_to_home(animal_dicts)
            self.processed_count += len(animals)
            logger.info(f"Successfully loaded {len(animals)} animals")
            return True
        except Exception as e:
            logger.error(f"Failed to load batch: {e}")
            return False

    async def run_pipeline(self) -> int:
        logger.info("Starting Animal ETL Pipeline...")
        start_time = datetime.now()

        try:
            animal_ids = await self.extract_all_animal_ids()

            if not animal_ids:
                logger.warning("No animal IDs found")
                return 0

            for i in range(0, len(animal_ids), self.batch_size):
                batch_ids = animal_ids[i:i + self.batch_size]
                logger.info(
                    f"Processing batch {i // self.batch_size + 1}/{(len(animal_ids) + self.batch_size - 1) // self.batch_size}")

                # Extract and transform batch
                animals_batch = await self.process_animals_batch(batch_ids)

                # Load batch
                if animals_batch:
                    await self.load_animals_batch(animals_batch)

            duration = datetime.now() - start_time
            logger.info(f"Pipeline completed successfully in {duration}")
            logger.info(f"Total animals processed: {self.processed_count}")

            return self.processed_count

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


async def main():
    logging.getLogger().setLevel(logging.DEBUG)

    try:
        async with AnimalAPIClient() as client:
            try:
                logger.info("Testing API connection...")
                test_response = await client.get_animals_page(1)
                logger.info(f"API test successful. Response keys: {list(test_response.keys())}")
                logger.debug(f"Full test response: {test_response}")
            except Exception as e:
                logger.error(f"API connection test failed: {e}")
                logger.error("Make sure the Docker container is running on port 3123")
                return

            pipeline = AnimalETLPipeline(client)
            processed_count = await pipeline.run_pipeline()
            print(f"Successfully processed {processed_count} animals")

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
