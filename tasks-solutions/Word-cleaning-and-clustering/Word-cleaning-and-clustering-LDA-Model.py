# # # # # # # # # # # # # # # # # # # # # 
# Очистка ключевых слов и кластеризация #
# # # # # # # # # # # # # # # # # # # # #

import re, logging
import gensim.downloader as api
from gensim import corpora
from gensim.models import LdaModel, LdaMulticore
from gensim.utils import simple_preprocess, lemmatize
from nltk.corpus import stopwords
from pprint import pprint

# Директория проекта
# dir = 'd:/Library/Git/Data_Science/tasks-and-solutions/Word-cleaning-and-clustering'

# Загружаем необходимые пакеты
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s')
logging.root.setLevel(level=logging.INFO)

# Импортируем стоп-слова
stop_words = stopwords.words('english')
stop_words = stop_words + ['com', 'edu', 'subject', 'lines', 'organization', 'would', 'article', 'could']

# Step 1: Импортируем набор данных и получаем текст и реальную тему каждой новостной статьи
# Необходимо найти и загрузить русскоязычный набор данных!!!
# Полный список доступных наборов данных и моделей поддерживается здесь:
# https://raw.githubusercontent.com/RaRe-Technologies/gensim-data/master/list.json

dataset = api.load("text8") # около 30 Мб
data = [d for d in dataset]

# Step 2: Подготовливаем данные (удаляем стоп-слова и лемматизируем)
# Части речи: существительные (NN), прилагательные (JJ), местоимения (RB)
data_processed = []
for i, doc in enumerate(data[:100]):
    doc_out = []
    for wd in doc:
        if wd not in stop_words:  # удаляем стоп-слова
            lemmatized_word = lemmatize(wd, allowed_tags=re.compile('(NN|JJ|RB)'))  # лемматизируем
            if lemmatized_word:
                doc_out = doc_out + [lemmatized_word[0].split(b'/')[0].decode('utf-8')]
        else:
            continue
    data_processed.append(doc_out)

# Выводим небольшой фрагмент    
print(data_processed[0][:5]) 

# Step 3: Создаем на вход для модели LDA: словарь и корпус
dct = corpora.Dictionary(data_processed)
corpus = [dct.doc2bow(line) for line in data_processed]

# Step 4: Используем LDA модель для обработки данных:
lda_model = LdaMulticore(corpus=corpus,
                         id2word=dct,
                         random_state=100,
                         num_topics=7,
                         passes=10,
                         chunksize=1000,
                         batch=False,
                         alpha='asymmetric',
                         decay=0.5,
                         offset=64,
                         eta=None,
                         eval_every=0,
                         iterations=100,
                         gamma_threshold=0.001,
                         per_word_topics=True)

# Сохраняем модель на диск
lda_model.save('lda_model.model')

# Выводим полученные данные
lda_model.print_topics(-1)


