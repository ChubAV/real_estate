import asyncio
from SpiderAvito import SpiderAvito

if __name__ == '__main__':
    x = SpiderAvito('http://nsprus.ru/')
    y = SpiderAvito('http://www.transferfaktory.ru')
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(x.getHtml()), loop.create_task(y.getHtml())]
    wait_tasks = asyncio.wait(tasks)
    loop.run_until_complete(wait_tasks)
    loop.close()
