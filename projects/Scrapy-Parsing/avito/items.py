# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from scrapy.loader.processors import MapCompose, TakeFirst      #Подключаем обраотчики
import scrapy

def cleaner_photo(value):               #Функция для изменения ссылок фотографий
    if value[:2] == '//':
        return f'http:{value}'
    return value

class AvitoItem(scrapy.Item):
    # define the fields for your item here like:
    _id = scrapy.Field()
    name = scrapy.Field(output_processor=TakeFirst())                   #Применяем обработчики
    photos = scrapy.Field(input_processor=MapCompose(cleaner_photo))

