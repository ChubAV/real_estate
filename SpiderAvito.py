import aiohttp

class SpiderAvito():

    def __init__(self, url):
        self.url = url

    async def getHtml(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as resp:
                print(self.url, '-', resp.status)