import asyncio
from src.ods_api import OdsApiClient

async def test():
    client = OdsApiClient()
    result = await client.list_datasets(limit=1)
    print(result)

if __name__ == "__main__":
    asyncio.run(test())