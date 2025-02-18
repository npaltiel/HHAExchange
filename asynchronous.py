import aiohttp
import asyncio


async def async_soap_request(url, payload, action):
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': action
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=headers) as response:
            if response.status != 200:
                print(f"Error {response.status}: {await response.text()}")
            return await response.text()
