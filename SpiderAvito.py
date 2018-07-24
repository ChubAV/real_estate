import aiohttp
#import random
import urllib3
import certifi
import re
from bs4 import BeautifulSoup

#from Settings import DB_SETTINGS, USER_AGENTS

class SpiderAvito():

    def __init__(self, url, user_agent=None, proxy=None):
        self.url = url
        if user_agent is None:
            self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                              'Chrome/67.0.3396.99 Safari/537.36 '
        else:
            self.user_agent = user_agent
        self.proxy = proxy

    def getHtml(self, url=None, referer=''):
        url_from_request = self.url if url is None else url
        headers_setting = {
            "User-Agent": self.user_agent,
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'referer': referer}
        if self.proxy is None:
            http = urllib3.PoolManager(
                cert_reqs='CERT_REQUIRED',
                ca_certs=certifi.where()
            )
        else:
            http = urllib3.ProxyManager(self.proxy,
                                        cert_reqs='CERT_REQUIRED',
                                        ca_certs=certifi.where()
                                        )
        r = http.request('GET', url_from_request, headers=headers_setting, redirect=False)
        if r.status != 200:
            raise urllib3.exceptions.HTTPError('HttpRequest status ' + str(r.status))
        return r.data

    @staticmethod
    def getOnlyNumber(number):
        reg_only_digital = re.compile('[^0-9]')
        return reg_only_digital.sub('', number)

    def getIP(self):
        url_2ip = 'https://2ip.ru/'
        soup = BeautifulSoup(self.getHtml(url_2ip), 'html.parser')
        return soup.find("big", {"id": "d_clip_button"}).text.strip()

    async def AIO_getHtml(self, url=None, referer=''):
        url_from_request = self.url if url is None else url
        headers_setting = {
            "User-Agent": self.user_agent,
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'referer': referer}
        async with aiohttp.ClientSession(headers=headers_setting) as session:
            async with session.get(url_from_request, proxy=self.proxy, allow_redirects=False) as resp:
                assert resp.status == 200
                print(url_from_request, '-', resp.status)
                return await resp.text()