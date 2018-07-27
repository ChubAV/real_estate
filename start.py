
from SpiderAvito import  SpiderAvitoDispatcherList
from Settings import DB_SETTINGS, USER_AGENTS
import time

URL_KVARTIRY_LIST='https://www.avito.ru/krasnodar/kvartiry/prodam'


def synchronously():
    x = SpiderAvitoDispatcherList(url=URL_KVARTIRY_LIST, user_agents=USER_AGENTS, db_setting=DB_SETTINGS, count_page=3)
    start_time = time.time()
    result = x.start()
    print(result)
    print(time.time() - start_time)


def asynchronously():
    x = SpiderAvitoDispatcherList(url=URL_KVARTIRY_LIST, user_agents=USER_AGENTS, db_setting=DB_SETTINGS, count_page=3)
    start_time = time.time()
    result = x.start_aio()
    print(result)
    print(time.time() - start_time)


if __name__ == '__main__':
    #synchronously()
    #time.sleep(10)
    asynchronously()

