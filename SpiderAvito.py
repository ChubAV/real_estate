import aiohttp
import asyncio
import random
import urllib3
import certifi
import re
from bs4 import BeautifulSoup
import traceback
import asyncpg
import postgresql
import itertools
import time


#from Settings import DB_SETTINGS, USER_AGENTS

class SpiderAvito(object):

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
        #else: print(url_from_request, '-', r.status)
        return r.data

    @staticmethod
    def getOnlyNumber(number):
        reg_only_digital = re.compile('[^0-9]')
        return reg_only_digital.sub('', number)

    def getIP(self):
        url_2ip = 'https://2ip.ru/'
        soup = BeautifulSoup(self.getHtml(url_2ip), 'html.parser')
        return soup.find("big", {"id": "d_clip_button"}).text.strip()

    async def getHtml_aio(self, url=None, referer=''):
        url_from_request = self.url if url is None else url
        headers_setting = {
            "User-Agent": self.user_agent,
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'referer': referer}
        async with aiohttp.ClientSession(headers=headers_setting) as session:
            async with session.get(url_from_request, proxy=self.proxy, allow_redirects=False) as resp:
                assert resp.status == 200
                #print(url_from_request, '-', resp.status)
                return await resp.text()


class SpiderAvitoList(SpiderAvito):
    __class_item_avito_list = 'item_table-description'  # класс для поиска объявление в авито
    __class_ads_link = 'item-description-title-link'  # класс ссылки на объявление
    __class_ads_price = 'price'  # класс цены объявления
    __class_ads_address = 'address'  # класс адрес объявления
    __class_ads_data = 'data'  # класс доп. данных объявления

    def __init__(self, url, user_agent=None, proxy=None, db_setting=None):
        super().__init__(url, user_agent, proxy)
        self.db_setting = db_setting

    def parsing(self, soup):
        items = soup.find_all('div', class_=self.__class_item_avito_list)
        if items is None:  # если блок не найден значит avito что-то поменял
            raise urllib3.exceptions.HTTPError('Tag div with "class_item_avito_list" not found' + self.url)
        for item in items:
            description_link = item.find('a', class_=self.__class_ads_link)
            if description_link is None:  # если блок не найден значит avito что-то поменял
                raise urllib3.exceptions.HTTPError('Tag "a" with "class_ads_link" not found' + self.url)
            result_item = {'url': 'https://www.avito.ru' + description_link['href'],
                           'title': description_link['title'],
                           'price': self.getOnlyNumber(item.find('span', class_=self.__class_ads_price).text),
                           'address': item.find('p', class_=self.__class_ads_address).text.strip(),
                           'data': item.find('div', class_=self.__class_ads_data).text.strip()
                           }
            yield result_item
        raise StopIteration

    def start(self):
        html = self.getHtml()
        soup = BeautifulSoup(html, 'html.parser')
        parser = self.parsing(soup)
        count_insert = 0
        count_not_insert = 0
        for item in parser:
            result_insert = self.save_to_postgresql(item)
            if result_insert is True:
                count_insert += 1
            else:
                count_not_insert += 1
        #print(count_insert, count_not_insert)
        return (count_insert, count_not_insert)

    async def start_aio(self):
        html = await self.getHtml_aio()
        soup = BeautifulSoup(html, 'html.parser')
        parser = self.parsing(soup)
        count_insert = 0
        count_not_insert = 0
        for item in parser:
           result_insert = await self.save_to_postgresql_aio(item)
           if result_insert is True:
               count_insert += 1
           else:
               count_not_insert += 1
        #print(count_insert, count_not_insert)
        return (count_insert, count_not_insert)

    async def save_to_postgresql_aio(self, item):
        username, password, host, port, dbname = self.db_setting.values()
        conn = await asyncpg.connect('postgresql://'+username+':'+password+'@'+host+':'+port+'/'+dbname)
        ins = await conn.prepare('INSERT INTO ads(url, title, price, data, address, date_add) VALUES ($1, $2, $3, $4, $5, now())')
        in_base_true = await conn.prepare('SELECT * FROM ads where url=$1')
        if await in_base_true.fetchval(item['url']) is None:
            await ins.fetch(item['url'], item['title'], item['price'], item['data'], item['address'])
            return True
        else:
            return False

    def save_to_postgresql(self, item):
        username, password, host, port, dbname = self.db_setting.values()
        db = postgresql.open('pq://' + username + ':' + password + '@' + host + ':' + port + '/' + dbname)
        ins = db.prepare('INSERT INTO ads(url, title, price, data, address, date_add) VALUES ($1, $2, $3, $4, $5, now())')  # запрос на вставку в базу
        in_base_true = db.prepare("SELECT * FROM ads where url=$1")  # проверка есть ли это объявление в базе
        if len(in_base_true(item['url'])) == 0:
            ins(item['url'], item['title'], item['price'], item['data'], item['address'])
            return True
        else:
            return False

class SpiderAvitoDispatcherList():

    def __init__(self, url, db_setting, user_agents=None, proxies=None, count_page=10, time_sleep=True):
        self.CountPage = count_page
        self.user_agents = user_agents if user_agents is not None else list('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36')
        self.proxies = proxies
        self.db_setting = db_setting
        self.url = url
        self.time_sleep = time_sleep

    def next_proxy(self):
        l = itertools.cycle(self.proxies) if self.proxies is not None else None
        while True:
            if l is None:
                yield None
            else:
                yield next(l)

    def next_page(self, *args):
        print('Парсинг страницы:', self.url)
        yield self.url
        page = 1
        while True:
            page += 1
            result = self.url + '?p=' + str(page)
            print('Парсинг страницы:', result)
            yield result

    def start(self):
        result = 0
        proxy_iter = self.next_proxy()
        page_iter = self.next_page()
        for page in range(1, self.CountPage + 1):
            result_item = 0
            proxy = next(proxy_iter)
            x = SpiderAvitoList(next(page_iter), random.choice(self.user_agents), proxy, self.db_setting)
            try:
                result_item, _ = x.start()
                result += result_item
            except Exception as err:
                print('================================================================')
                print('Произошла ошибка при построении списка объявлений: ', x.url, err)
                traceback.print_exc()
                print('================================================================')
            if result_item == 0: break
            if self.time_sleep: time.sleep(random.randint(5, 10))
        return result

    def start_aio(self):
        result = 0
        proxy_iter = self.next_proxy()
        page_iter = self.next_page()
        count_proxies = len(self.proxies) if self.proxies is not None else 1
        more_page = self.CountPage
        loop = asyncio.get_event_loop()
        while more_page > 0:
            result_item = 0
            spiders = [SpiderAvitoList(i, random.choice(self.user_agents), next(proxy_iter), self.db_setting)
                        for i in list(map(lambda x: next(page_iter), list(range(1, min(more_page+1, count_proxies+1)))))
                     ]
            tasks = [loop.create_task(a.start_aio()) for a in spiders]
            wait_tasks = asyncio.wait(tasks)
            try:
                loop.run_until_complete(wait_tasks)
                result_item = sum(map(lambda x: x.result()[0], tasks))
                result += result_item
            except Exception as err:
                print('================================================================')
                print('Произошла ошибка при построении списка объявлений: ', list(i.url for i in spiders), err)
                traceback.print_exc()
                print('================================================================')
            if result_item == 0: break
            if self.time_sleep: time.sleep(random.randint(5, 10))
            more_page -= len(spiders)

        loop.close()
        return result
