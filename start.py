import asyncio
import random
from SpiderAvito import SpiderAvito
from Settings import DB_SETTINGS, USER_AGENTS
import time
if __name__ == '__main__':
    x = SpiderAvito('https://ya.ru/', random.choice(USER_AGENTS))
    y = SpiderAvito('https://www.avito.ru/krasnodar/kvartiry/prodam', random.choice(USER_AGENTS))
    start__time = time.time()
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(x.AIO_getHtml()), loop.create_task(y.AIO_getHtml())]
    wait_tasks = asyncio.wait(tasks)
    loop.run_until_complete(wait_tasks)
    loop.close()
    print(time.time()-start__time)
    #x.getHtml()
    #y.getHtml()
    #print(time.time() - start__time)