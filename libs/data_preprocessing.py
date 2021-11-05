'''

# Library for preprocessing datasets
# Библиотека для предобработки датасетов
# Кладем ее рядом в файл data_preprocessing.py

# Импорты в основной Jupyter Notebook:
# Библиотека для предобработки 
import os, sys
module_path = os.path.abspath(os.path.join(os.pardir))
if module_path not in sys.path:
    sys.path.append(module_path)
from data_preprocessing import DataPreprocessor
dp = DataPreprocessor()

# Использование функций в Jupyter Notebook:
df = dp.myfunc(df)

'''

import numpy as np
import pandas as pd
import re
from transliterate import translit, get_available_language_codes
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, f1_score, accuracy_score, precision_score, recall_score
from sklearn.model_selection import train_test_split


class DataPreprocessor:

    def basic_analysis(self, df, df_name, stat):
        '''Базовый анализ по датасетам

        >>> stat = pd.DataFrame(columns=['train', 'test'])
        >>> dp.basic_analysis(df=train, df_name='train', stat=stat)
        >>> dp.basic_analysis(df=test, df_name='test', stat=stat)
        >>> stat
        '''
        stat.loc['Количество наблюдений', df_name] = df.shape[0]
        stat.loc['Количество признаков', df_name] = df.shape[1]
        
        for k, v in df.dtypes.value_counts().items():
            stat.loc[f'Признаков {k}', df_name] = v
            
        list_nan = [i for i in df.count() if i != df.shape[0]]
        stat.loc[f'Признаков с NaN', df_name] = len(list_nan)
        
        dict_num = {'disc': 0, 'cont': 0}
        for i in df.columns:
            if df[i].dtype != 'object':
                if df[i].nunique() < 25:
                    dict_num['disc'] += 1
                else:
                    dict_num['cont'] += 1
                    
        stat.loc[f'Дискретных признаков', df_name] = dict_num['disc']
        stat.loc[f'Непрерывных признаков', df_name] = dict_num['cont']


    def groups_features(self, df, TARGET=None):
        '''Группировка базовых признаков по типам

        TARGET          -- целевая переменная
        BASE_FEATURES   -- все признаки без TARGET
        DATE_FEATURES   -- временные признаки
        NUM_FEATURES    -- вещественные признаки
        CAT_FEATURES    -- категориальные признаки

        >>> BASE_FEATURES, DATE_FEATURES, NUM_FEATURES, CAT_FEATURES = dp.groups_features(df)
        >>> BASE_FEATURES, DATE_FEATURES, NUM_FEATURES, CAT_FEATURES = dp.groups_features(df, TARGET='price')
        '''
        data = df.copy()
        if TARGET: data.drop(TARGET, axis=1, inplace=True)
        BASE_FEATURES = data.columns.tolist()
        DATE_FEATURES = data.select_dtypes(include=[np.datetime64]).columns.tolist()
        DF_NUM_FEATURES = data.select_dtypes(include=[np.number])
        if DATE_FEATURES: NUM_FEATURES = [feature for feature in DF_NUM_FEATURES if feature not in DATE_FEATURES]
        else: NUM_FEATURES = data.select_dtypes(include=[np.number]).columns.tolist()
        CAT_FEATURES = data.select_dtypes(include=[np.object]).columns.tolist()
        return BASE_FEATURES, DATE_FEATURES, NUM_FEATURES, CAT_FEATURES


    def groups_all_features(self, df, TARGET=None):
        '''Группировка всех признаков по типам

        TARGET          -- целевая переменная
        BASE_FEATURES   -- все признаки без TARGET
        DATE_FEATURES   -- временные признаки
        NUM_FEATURES    -- вещественные признаки
        DISC_FEATURES   -- дискретные признаки
        CONT_FEATURES   -- непрерывные признаки
        CAT_FEATURES    -- категориальные признаки

        >>> BASE_FEATURES, DATE_FEATURES, NUM_FEATURES, DISC_FEATURES, CONT_FEATURES, CAT_FEATURES = dp.groups_features(df)
        >>> BASE_FEATURES, DATE_FEATURES, NUM_FEATURES, DISC_FEATURES, CONT_FEATURES, CAT_FEATURES = dp.groups_features(df, TARGET='price')
        '''
        data = df.copy()
        if TARGET: data.drop(TARGET, axis=1, inplace=True)
        BASE_FEATURES = data.columns.tolist()
        DATE_FEATURES = data.select_dtypes(include=[np.datetime64]).columns.tolist() 
        DF_NUM_FEATURES = data.select_dtypes(include=[np.number])
        if DATE_FEATURES: NUM_FEATURES = [feature for feature in DF_NUM_FEATURES if feature not in DATE_FEATURES]
        else: NUM_FEATURES = data.select_dtypes(include=[np.number]).columns.tolist()
        if NUM_FEATURES:
            DISC_FEATURES = [feature for feature in DF_NUM_FEATURES if data[feature].nunique()<25 and feature not in DATE_FEATURES]
            CONT_FEATURES = [feature for feature in DF_NUM_FEATURES if feature not in DISC_FEATURES + DATE_FEATURES]
        CAT_FEATURES = data.select_dtypes(include=[np.object]).columns.tolist()
        return BASE_FEATURES, DATE_FEATURES, NUM_FEATURES, DISC_FEATURES, CONT_FEATURES, CAT_FEATURES


    def astype_col(self, df, colgroup, coltype):
        '''Приведение типов
        
        colgroup    -- список фич
        coltype     -- тип данных

        >>> df = dp.astype_col(df, ['colgroup'], 'coltype')
        '''
        for colname in colgroup:
            df[colname] = df[colname].astype(coltype)
        return df


    def drop_col(self, df, colgroup):
        '''Удаление столбцов
        
        >>> df = dp.drop_col(df, colgroup)
        '''
        for column in colgroup:
            df = df.drop(column, axis=1)  
        return df


    def get_translite(self, colgroup, prefix=None):
        '''Транслитерация имен столбцов

        colgroup    -- список фич
        prefix_     -- префикс для группировки фич
        
        >>> df.columns = dp.get_translite(df.columns, 'prefix_')
        '''
        lst = []
        for column in colgroup:
            r = translit(prefix + column, 'ru', reversed=True).lower()
            r = re.sub("\'|\(|\)|:", "", r)
            r = re.sub("\ |-|‑", "_", r)
            r = r.replace(".", "_")
            lst.append(r)
        return lst


    def get_idx_col(self, colgroup):
        '''Получение имен столбцов с индексом
        
        >>> df = dp.get_idx_col(colgroup)
        '''
        lst = []
        for index, value in enumerate(colgroup):
            print(index, value)
            lst.append(index)
        print(lst)


    def normalization_df(self, df, colgroup):
        '''Нормализация данных

        >>> df = dp.normalization_df(df, NUM_FEATURES)
        '''
        scaler = StandardScaler()
        df_norm = df.copy()
        df_norm[colgroup] = scaler.fit_transform(df_norm[colgroup])
        df = df_norm.copy()
        return df


    def dummies_col(self, df, colgroup):
        '''Перевод категориальных признаков в dummies

        >>> df = dp.dummies_col(df, CAT_FEATURES)
        '''
        for colname in colgroup:
            df = pd.concat([df, pd.get_dummies(df[colname], prefix=colname)], axis=1)
        return df


    def time_in_seconds(self, t):
        '''Преобразование времени в секунды

        Input: "00:10:00"
        Output: 600

        >>> df = dp.time_in_seconds(self, t)
        '''
        h, m, s = [int(i) for i in t.split(':')]
        return 3600*h + 60*m + s