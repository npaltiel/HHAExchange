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


async def retry_soap_request(endpoint_url, payload, action, max_retries=3, delay=2):
    async def make_request():
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': action
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint_url, data=payload, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}: {await response.text()}")
                return await response.text()

    return await retry_request(make_request, max_retries=max_retries, delay=delay)


async def retry_request(request_func, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            return await request_func()
        except Exception as e:
            # print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(delay * (2 ** attempt))
            else:
                raise
