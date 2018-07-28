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
from datetime import datetime, timedelta
import json
import sys


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
        reg_only_digital = re.compile('[^0-9\.]')
        result = reg_only_digital.sub('', number)
        return result if len(result) > 0 else 0

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
        #raise StopIteration

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
            await conn.close()
            return True
        else:
            await conn.close()
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

class SpiderAvitoDispatcher(object):
    def __init__(self, db_setting, user_agents=None, proxies=None, count_page=10, time_sleep=True):
        self.CountPage = count_page
        self.user_agents = user_agents if user_agents is not None else list(
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36')
        self.proxies = proxies
        self.db_setting = db_setting
        self.time_sleep = time_sleep

    def next_proxy(self):
        l = itertools.cycle(self.proxies) if self.proxies is not None else None
        while True:
            if l is None:
                yield None
            else:
                yield next(l)
class SpiderAvitoDispatcherList(SpiderAvitoDispatcher):
    def __init__(self, url, db_setting, user_agents=None, proxies=None, count_page=10, time_sleep=True):
        super().__init__(db_setting, user_agents, proxies, count_page, time_sleep)
        self.url = url

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
                traceback.print_exc(file=sys.stdout)
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
                traceback.print_exc(file=sys.stdout)
                print('================================================================')
            if result_item == 0: break
            if self.time_sleep: time.sleep(random.randint(5, 10))
            more_page -= len(spiders)

        loop.close()
        return result


class SpiderAvitoAds(SpiderAvito):
    def __init__(self, url, user_agent=None, proxy=None, db_setting=None):
        super().__init__(url=url, user_agent=user_agent, proxy=proxy)
        self.db_setting = db_setting
        self.FullAddress = ''
        self.CountRoom = ''
        self.Floor = ''
        self.CountFoor = ''
        self.Material = ''
        self.Area = ''
        self.AreaKitchen = ''
        self.AreaLife = ''
        self.Price = ''
        self.Images = []
        self.NumberAds = ''
        self.GPS = None
        self.TextAds = ''
        self.AuthorAds = ''
        self.DateAdd = ''
        self.isAgent = False
        self.Phone = ''
    #####################################################################################
    ##### Возвращает Адрес объекта указанный на странице avito пока не разделенный
    def getAddressFromPage(self, soup):
        result=soup.find_all('div', class_='seller-info-prop')[-1].find('div', class_='seller-info-value').text.strip()
        #result=result.split(',')
        return result
    #####################################################################################
    ##### Возвращает автора объявления
    def getAuhtorFromPage(self, soup):
        return soup.find_all('div', class_='seller-info-prop')[0].a.text.strip()
    #####################################################################################
    ##### Это агентство?
    def isAgentFromPage(self, soup):
        if 'Агентство' in soup.find_all('div', class_='seller-info-prop')[0].text:
            return True
        else:
            return False
    #####################################################################################
    ##### Возвращает количество комнат
    def getCountRoomFromPage(self, soup):
        find_text='Количество комнат'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                result = item.text.strip()
                result= self.getOnlyNumber(result)
                if result=='': result=0
                return result
        return 0

    #####################################################################################
    ##### Возвращает этаж на котором располагается квартира
    def getFloorFromPage(self, soup):
        find_text='Этаж:'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                result = item.text.strip()
                result= self.getOnlyNumber(result)
                return result
        return ''

    #####################################################################################
    ##### Возвращает количество этажей в доме
    def getCountFloorFromPage(self, soup):
        find_text='Этажей в доме'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                result = item.text.strip()
                result= self.getOnlyNumber(result)
                return result
        return ''
    #####################################################################################
    ##### Возвращает материал конструктивных элементов здания
    def getMaterialFromPage(self, soup):
        find_text='Тип дома'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                result = item.text.strip().split(': ')[-1]
                return result
        return ''

    #####################################################################################
    ##### Возвращает Общую площадь квартиры
    def getAreaFromPage(self, soup):
        find_text='Общая площадь'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                return item.text.strip().split(': ')[-1].split('м')[0].strip()
        return '0'
    #####################################################################################
    ##### Возвращает Площадь кухни
    def getAreaKitchenFromPage(self, soup):
        find_text='Площадь кухни'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                return item.text.strip().split(': ')[-1].split('м')[0].strip()
        return '0'
    #####################################################################################
    ##### Возвращает Жилая площадь
    def getAreaLifeFromPage(self, soup):
        find_text='Жилая площадь'
        items=soup.find_all('li', class_='item-params-list-item')
        for item in items:
            if find_text in item.text:
                return item.text.strip().split(': ')[-1].split('м')[0].strip()
        return '0'

    #####################################################################################
    ##### Возвращает цену объекта
    def getPriceFromPage(self, soup):
        result = soup.find_all('span', class_='js-item-price')[0].text.strip()
        result= self.getOnlyNumber(result)
        return result
    #####################################################################################
    ##### Возвращает Номер объявления
    def getNumberAdsFromPage(self, soup):
        try:
            result = soup.find_all('div', class_='title-info-metadata-item')[0].text.split(',')[0].strip()
            result= self.getOnlyNumber(result)
        except Exception:
            result='0000000000'
        return result
    #####################################################################################
    ##### Возвращает Дату добавления объявления
    def getDateAddFromPage(self, soup):
        mounthDict={
        'января':'01',
        'февраля':'02',
        'марта':'03',
        'апреля':'04',
        'мая':'05',
        'июня':'06',
        'июля':'07',
        'августа':'08',
        'сентября':'09',
        'октября':'10',
        'ноября':'11',
        'декабря':'12',
        }
        today=datetime.today()
        try:
            result = soup.find_all('div', class_='title-info-metadata-item')[0].text.split(',')[-1].strip().split('азмещено ')[-1].split(' в ')
            yesterday = datetime.today() - timedelta(1)
            z = '{:%d.%m.%Y} {}'.format(today, result[1]) if result[0]=='сегодня' else '{:%d.%m.%Y} {}'.format(yesterday, result[1]) if result[0]=='вчера' else result[0].split(' ')[0]+'.'+mounthDict[result[0].split(' ')[1]]+'.'+str(today.year)+' '+result[1]
        except Exception:
            z = '{:%d.%m.%Y 00:00}'.format(today)
        return z
    #####################################################################################
    ##### Возвращает url на картинки объекта
    def getImageFromPage(self, soup):
        items = soup.find_all('div', class_='gallery-extended-img-frame') #[0]['data-url']
        result = []
        for item in items:
            result.append(item['data-url'].strip()[2:])
        return result
    #####################################################################################
    ##### Возвращает кординаты объекта на карте
    def getGPSFromPage(self, soup):
        return (soup.find_all('div', class_='b-search-map')[0]['data-map-lat'], soup.find_all('div', class_='b-search-map')[0]['data-map-lon'])
    #####################################################################################
    ##### Возвращает текстовую часть объявления
    def getTextAdsFromPage(self, soup):
        return soup.find_all('div', class_='item-description')[0].text.strip()

    def getPhoneFromPage(self):
        try:
            mobile_url=self.url.replace("www", "m")

            html=self.getHtml(mobile_url)
            soup=BeautifulSoup(html, 'html.parser')

            url_phone='https://m.avito.ru'+soup.find_all('a', class_='person-action')[0]['href']+'?async'

            html=self.getHtml(url_phone, mobile_url)
            result=json.loads(html.decode('utf-8'))['phone']
        except Exception:
            return ''

        return '+'+self.getOnlyNumber(result)

    def isActive(self, soup):
        title=soup.find_all('title')[0].text
        return ('отклонено' not in  title.lower()) #and ('удалено' not in  'удалено')

    def parsing(self, soup):
        self.FullAddress = self.getAddressFromPage(soup)
        self.CountRoom = self.getCountRoomFromPage(soup)
        self.Floor = self.getFloorFromPage(soup)
        self.CountFoor = self.getCountFloorFromPage(soup)
        self.Material = self.getMaterialFromPage(soup)
        self.Area = self.getAreaFromPage(soup)
        self.AreaKitchen = self.getAreaKitchenFromPage(soup)
        self.AreaLife = self.getAreaLifeFromPage(soup)
        self.Price = self.getPriceFromPage(soup)
        self.NumberAds = self.getNumberAdsFromPage(soup)
        self.Images.extend(self.getImageFromPage(soup))
        self.GPS = self.getGPSFromPage(soup)
        self.TextAds = self.getTextAdsFromPage(soup)
        self.AuthorAds = self.getAuhtorFromPage(soup)
        self.isAgent = self.isAgentFromPage(soup)
        self.DateAdd = self.getDateAddFromPage(soup)
        self.Phone = self.getPhoneFromPage()

    def getListProperty(self):
        return (
            self.url,
            self.FullAddress,
            self.CountRoom,
            self.Floor,
            self.CountFoor,
            self.Material,
            self.Area,
            self.AreaKitchen,
            self.AreaLife,
            self.Price,
            self.NumberAds,
            self.Images,
            self.GPS,
            self.TextAds,
            self.AuthorAds,
            self.isAgent,
            self.DateAdd,
            self.Phone,
                )

    def start(self):
        print('Парсинг - ', self.url)
        try:
            html = self.getHtml()
        except urllib3.exceptions.HTTPError as err:
            self.deactive_to_postgresql()
            raise urllib3.exceptions.HTTPError()
        soup = BeautifulSoup(html, 'html.parser')
        if self.isActive(soup) is False:
            self.deactive_to_postgresql()
            raise urllib3.exceptions.HTTPError()
        self.parsing(soup)
        #print(self.getListProperty())
        self.save_to_postgresql()
        return (self.FullAddress, self.AuthorAds, self.Phone)

    async def start_aio(self):
        print('Парсинг (async) - ', self.url)
        try:
            html = await self.getHtml_aio()
        except urllib3.exceptions.HTTPError as err:
            await self.deactive_to_postgresql_aio()
            raise urllib3.exceptions.HTTPError()

        soup = BeautifulSoup(html, 'html.parser')

        if self.isActive(soup) is False:
            await self.deactive_to_postgresql_aio()
            raise urllib3.exceptions.HTTPError()

        self.parsing(soup)
        await self.save_to_postgresql_aio()
        return (self.FullAddress, self.AuthorAds, self.Phone)

    def save_to_postgresql(self):
        username, password, host, port, dbname = self.db_setting.values()
        db = postgresql.open('pq://'+username+':'+password+'@'+host+':'+port+'/'+dbname)
        querySave = db.prepare('update ads set "FullAddress"=$1, "CountRoom"=$2, "Floor"=$3, "CountFoor"=$4, "Material"=$5, "Area"=$6, "AreaKitchen"=$7, "AreaLife"=$8, "NumberAds"=$9, "Images"=$10, "TextAds"=$11, "AuthorAds"=$12, "isAgent"=$13, "Phone"=$14, "isLoad"=true, "Gps"=point($15, $16), date_add_site=$17 where url=$18')
        querySave(self.FullAddress,
                  int(self.CountRoom),
                  self.Floor,
                  self.CountFoor,
                  self.Material,
                  float(self.getOnlyNumber(self.Area)),
                  float(self.getOnlyNumber(self.AreaKitchen)),
                  float(self.getOnlyNumber(self.AreaLife)),
                  self.NumberAds,
                  ';'.join(self.Images),
                  self.TextAds,
                  self.AuthorAds,
                  self.isAgent ,
                  self.Phone,
                  float(self.getOnlyNumber(self.GPS[0])),
                  float(self.getOnlyNumber(self.GPS[1])),
                  datetime.strptime(self.DateAdd,'%d.%m.%Y %H:%M'),
                  self.url
                  )
        return True

    async def save_to_postgresql_aio(self):
        username, password, host, port, dbname = self.db_setting.values()
        conn = await asyncpg.connect(
            'postgresql://' + username + ':' + password + '@' + host + ':' + port + '/' + dbname)
        querySave = await conn.prepare(
            'update ads set "FullAddress"=$1, "CountRoom"=$2, "Floor"=$3, "CountFoor"=$4, "Material"=$5, "Area"=$6, "AreaKitchen"=$7, "AreaLife"=$8, "NumberAds"=$9, "Images"=$10, "TextAds"=$11, "AuthorAds"=$12, "isAgent"=$13, "Phone"=$14, "isLoad"=true, "Gps"=point($15, $16), date_add_site=$17 where url=$18')
        await querySave.fetch(
                            self.FullAddress,
                            int(self.CountRoom),
                            self.Floor,
                            self.CountFoor,
                            self.Material,
                            float(self.getOnlyNumber(self.Area)),
                            float(self.getOnlyNumber(self.AreaKitchen)),
                            float(self.getOnlyNumber(self.AreaLife)),
                            self.NumberAds,
                            ';'.join(self.Images),
                            self.TextAds,
                            self.AuthorAds,
                            self.isAgent,
                            self.Phone,
                            float(self.getOnlyNumber(self.GPS[0])),
                            float(self.getOnlyNumber(self.GPS[1])),
                            datetime.strptime(self.DateAdd, '%d.%m.%Y %H:%M'),
                            self.url
                )
        await conn.close()
        return True


    def deactive_to_postgresql(self):
        username, password, host, port, dbname = self.db_setting.values()
        db = postgresql.open('pq://'+username+':'+password+'@'+host+':'+port+'/'+dbname)
        querySave = db.prepare('update ads set "deactive"=true where url=$1')
        querySave(self.url)
        return True

    async def deactive_to_postgresql_aio(self):
        username, password, host, port, dbname = self.db_setting.values()
        conn = await asyncpg.connect(
            'postgresql://' + username + ':' + password + '@' + host + ':' + port + '/' + dbname)
        querySave = await conn.prepare('update ads set "deactive"=true where url=$1')
        querySave.fetch(self.url)
        await conn.close()
        return True

class SpiderAvitoDispatcherAds(SpiderAvitoDispatcher):
    def getListUrl(self):
        username, password, host, port, dbname = self.db_setting.values()
        db = postgresql.open('pq://' + username + ':' + password + '@' + host + ':' + port + '/' + dbname)
        query = db.prepare(
            'select id, url from ads where "isLoad"<>true and deactive=false order by random() limit $1')  # проверка есть ли это объявление в базе
        result = list()
        for item in query(self.CountPage):
            result.append(item['url'])
        return result

    def next_page(self, lst):
        for item in lst:
            yield item
        #raise StopIteration

    def start(self):
        iter_proxy = self.next_proxy()
        iter_page = self.next_page(self.getListUrl())
        result = 0
        for i in iter_page:
            proxy = next(iter_proxy)
            x = SpiderAvitoAds(url=i, user_agent=random.choice(self.user_agents),  proxy=proxy, db_setting=self.db_setting)
            try:
                if x.start() is not None: result += 1
            except Exception as err:
                print('================================================================')
                print('Произошла ошибка при парсинге объявления: ', x.getListProperty(), err)
                traceback.print_exc(file=sys.stdout)
                print('================================================================')
            if self.time_sleep: time.sleep(random.randint(5, 10))
        return result

    def start_aio(self):
        iter_proxy = self.next_proxy()
        lst = self.getListUrl()
        iter_page = self.next_page(lst)
        more_page = min(self.CountPage, len(lst))
        result = 0
        count_proxies = len(self.proxies) if self.proxies is not None else 1
        loop = asyncio.get_event_loop()
        while more_page > 0:
            spiders = [SpiderAvitoAds(url=i, user_agent=random.choice(self.user_agents),  proxy=next(iter_proxy), db_setting=self.db_setting)
                        for i in list(map(lambda x: next(iter_page), list(range(1, min(more_page+1, count_proxies+1)))))
                     ]
            tasks = [loop.create_task(a.start_aio()) for a in spiders]
            wait_tasks = asyncio.wait(tasks)
            try:
                loop.run_until_complete(wait_tasks)
                result += sum(map(lambda x: 0 if x.result()[0] is None else 1, tasks))
            except Exception as err:
                print('================================================================')
                print('Произошла ошибка при парсинге объявлений(async): ', err)
                for ii in spiders:
                    print(ii.getListProperty())
                traceback.print_exc(file=sys.stdout)
                print('================================================================')
            if self.time_sleep: time.sleep(random.randint(5, 10))
            more_page -= len(spiders)

        loop.close()
        return result
