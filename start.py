
from SpiderAvito import SpiderAvitoDispatcherList,  SpiderAvitoDispatcherAds
from Settings import DB_SETTINGS, USER_AGENTS
import time
import sys
import traceback

URL_KVARTIRY_LIST='https://www.avito.ru/krasnodar/kvartiry/prodam'


def asynchronously():
    start_time = time.time()
    count_while = 0
    result = 0
    try:
        while True:
            if count_while % 2 == 0:
                x = SpiderAvitoDispatcherList(url=URL_KVARTIRY_LIST, db_setting=DB_SETTINGS, user_agents=USER_AGENTS)
                result += x.start_aio()
            else:
                x = SpiderAvitoDispatcherAds(db_setting=DB_SETTINGS, user_agents=USER_AGENTS)
                result += x.start_aio()
            count_while += 1
    except KeyboardInterrupt:
        print('Пользователь прервал программу.')
    except Exception as err:
        print('Произошла ошибка - ', err)
        traceback.print_exc(file=sys.stdout)
    finally:

        print(time.time()-start_time, result)


def synchronously():
    start_time = time.time()
    count_while = 0
    result = 0
    try:
        while True:
            if count_while % 2 == 0:
                x = SpiderAvitoDispatcherList(url=URL_KVARTIRY_LIST, db_setting=DB_SETTINGS, user_agents=USER_AGENTS)
                result += x.start()
            else:
                x = SpiderAvitoDispatcherAds(db_setting=DB_SETTINGS, user_agents=USER_AGENTS)
                result += x.start()
            count_while += 1
    except KeyboardInterrupt:
        print('Пользователь прервал программу.')
    except Exception as err:
        print('Произошла ошибка - ', err)
        traceback.print_exc(file=sys.stdout)
    finally:

        print(time.time()-start_time, result)

if __name__ == '__main__':
    synchronously()
    #time.sleep(10)
    #asynchronously()


