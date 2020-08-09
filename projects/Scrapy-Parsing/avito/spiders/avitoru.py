# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import HtmlResponse
from avito.items import AvitoItem
from scrapy.loader import ItemLoader

class AvitoruSpider(scrapy.Spider):
    name = 'avitoru'
    allowed_domains = ['avito.ru']

    def __init__(self, search):
        self.start_urls = [f'https://www.avito.ru/rossiya?q={search[0]}',f'https://www.avito.ru/rossiya?q={search[1]}']

    def parse(self, response):
        ads_links = response.xpath('//h3/a[@class="snippet-link"]')
        for link in ads_links:
            yield response.follow(link, callback=self.parse_ads)

    def parse_ads(self, response:HtmlResponse):
        loader =ItemLoader(item=AvitoItem(),response=response)      #Работаем через item loader
        loader.add_xpath('photos','//div[contains(@class,"gallery-img-wrapper")]/div/@data-url')
        loader.add_xpath('name','//h1/span/text()')
        yield loader.load_item()

        # photos = response.xpath('//div[contains(@class,"gallery-img-wrapper")]/div/@data-url').extract()
        # name = response.xpath('//h1/span/text()').extract_first()
        # yield AvitoItem(name=name,photos=photos)

