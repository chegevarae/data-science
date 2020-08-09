from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from avito.spiders.avitoru import AvitoruSpider
from avito import settings

if __name__ == '__main__':
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)
    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(AvitoruSpider, search=['bmw x6','mercedes'])  #Передаем список параметров для поиска
    process.start()