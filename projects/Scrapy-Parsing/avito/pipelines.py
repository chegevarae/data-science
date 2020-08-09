# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
from scrapy.pipelines.images import ImagesPipeline

class DataBasePipeline:
    def process_item(self, item, spider):
        print(1)   #Здесь реализовать добавление в БД
        return item

class AvitoPhotosPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
       if item['photos']:
           for img in item['photos']:
               try:
                   yield scrapy.Request(img,meta=item)   #Скачиваем фото и передает item через meta
               except Exception as e:
                   print(e)

    # def file_path(self, request, response=None, info=None, ):
    #     item = request.meta             #Получаем item из meta
    #     return ''                       #Здесь необходимо вернуть путь для сохранения фотографий

    def item_completed(self, results, item, info):
        if results:
           item['photos']=[itm[1] for itm in results if itm[0]]
        return item


