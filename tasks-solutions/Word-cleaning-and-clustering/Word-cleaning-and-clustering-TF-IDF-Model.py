# # # # # # # # # # # # # # # # # # # # # 
# Очистка ключевых слов и кластеризация #
# # # # # # # # # # # # # # # # # # # # #

import gensim
import numpy as np
import gensim.downloader as api
from gensim import corpora
from gensim import models
from pprint import pprint


# Директория проекта
# dir = 'd:/Library/Git/Data_Science/tasks-solutions/Word-cleaning-and-clustering'

# Записываем все предложения построчно в список
documents = []
lst = open('keys.txt', 'r', encoding='utf-8')
for el in lst:
    documents.append(el)
lst.close()

# Токенизируем (разбиваем) предложения на слова
texts = [[text for text in doc.split()] for doc in documents]

# Создаем словарь
dictionary = corpora.Dictionary(texts)

# Получаем информацию о словаре
# print(dictionary)

# Выводим уникальные идентификаторы для каждого токена
# print(dictionary.token2id)

# Создаем корпус
mydict = corpora.Dictionary()
mycorpus = [mydict.doc2bow(doc, allow_update=True) for doc in texts]

# Если нужно будет преобразовать этот массив обратно в текст:
# word_counts = [[(mydict[id], count) for id, count in line] for line in mycorpus]
# pprint(word_counts)


# Сохраняем Словарь и Корпус
# mydict.save('mydict.dict')  # сохраняем словарь на диск
# corpora.MmCorpus.serialize('mycorpus.mm', mycorpus)  # сохраняем корпус на диск

# Загружаем словарь и корпус с диска
# mydict = corpora.Dictionary.load('mydict.dict')
# mycorpus = corpora.MmCorpus('mycorpus.mm')
# for line in mycorpus:
#     print(line)


# Используем TF-IDF модель для обработки данных:
tfidf = models.TfidfModel(mycorpus, smartirs='ntc')

# Выводим для просмотра TF-IDF веса слов
# for doc in tfidf[mycorpus]:
#     print([[mydict[id], np.around(freq, decimals=2)] for id, freq in doc])

# Теперь необходимо обработать и сохранить данные...
f = open('final.txt', 'w', encoding='utf-8')
for doc in tfidf[mycorpus]:
    words = ''
    weight = 0.0
    for id, freq in doc:
        words = words + ' ' + mydict[id]
        weight += np.around(freq, decimals=2)
    # print([[mydict[id], np.around(freq, decimals=2)] for id, freq in doc])
    # print(f'{words} - {weight:.2f}')
    if weight > 2.0:
        f.write(f'{words} - {weight:.2f}\n')
f.close()

# Можно провести дополнительную обработку, но для этого
# необходимо найти и загрузить русскоязычный набор данных!!!


# Создаем биграммы и триграммы с использованием моделей Phraser


# Получаем информацию о модели и наборе данных
# api.info('text8') # Первые 100 000 000 байтов простого текста из Википедии
# api.info('glove-wiki-gigaword-50') # Wikipedia 2014 + Gigaword 5 (6B tokens, uncased)

# Полный список доступных наборов данных и моделей поддерживается здесь:
# https://raw.githubusercontent.com/RaRe-Technologies/gensim-data/master/list.json


# # Загружаем модель по API
# dataset = api.load("text8")
# # dataset.most_similar('blue') # посмотреть для glove-wiki-gigaword-50
# dataset = [wd for wd in dataset]

# dct = corpora.Dictionary(dataset)
# corpus = [dct.doc2bow(line) for line in dataset]

# # Сборка биграмм моделей (долго)
# bigram = gensim.models.phrases.Phrases(dataset, min_count=3, threshold=10)

# # Посмотрим биграм
# # print(bigram[dataset[0]])

# # # Сборка модели триграмм (долго)
# trigram = gensim.models.phrases.Phrases(bigram[dataset], threshold=10)

# # # Посмотрим триграмм
# # print(trigram[bigram[dataset[0]]])



