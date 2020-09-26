########################################################################
## Преобразование лога в набор фич для дальнейшего использования в ML ##
########################################################################

import pandas as pd
import time, datetime
# from threading import Thread


# Сложность O(n)
def df_convert(df):
    '''
    Функция преобразования лог-файла в датасет.

    Принимает на вход лог-файл в виде датасета
    и преобразует этот лог-файл в набор фич.
    '''
    k = 100
    for i in range(len(df.index)):
        try:
            a = []
            tmp = df.values[i]

            # DataFrame: ProfileID,SiteID,ActionTime,ActionID,EmailID,ActionTimeDay

            a.append(tmp[0]) # ProfileID
            a.append(tmp[2]) # ActionTime

            # Генерация фич
            # Предварительные вычисления:

            # Получаем разницу между текущим временем и ActionTime
            atime = unix_current - tmp[2]

            # Получаем кол-во доставок/открытий/кликов
            if atime <= 86400: # за 24 часа
                cnt_doc_3 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 3) & (unix_current - df['ActionTime'] <= 86400), 'ActionID'])
                cnt_doc_3_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 3) & (unix_current - df['ActionTime'] <= 86400), 'ActionID'])
                cnt_doc_4 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 4) & (unix_current - df['ActionTime'] <= 86400), 'ActionID'])
                cnt_doc_4_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 4) & (unix_current - df['ActionTime'] <= 86400), 'ActionID'])
                cnt_doc_5 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 5) & (unix_current - df['ActionTime'] <= 86400), 'ActionID'])
                cnt_doc_5_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 5) & (unix_current - df['ActionTime'] <= 86400), 'ActionID'])
            if 86400 < atime <= 259200: # за 3 дня
                cnt_doc_3 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 3) & (86400 < unix_current - df['ActionTime'] <= 259200), 'ActionID'])
                cnt_doc_3_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 3) & (86400 < unix_current - df['ActionTime'] <= 259200), 'ActionID'])
                cnt_doc_4 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 4) & (86400 < unix_current - df['ActionTime'] <= 259200), 'ActionID'])
                cnt_doc_4_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 4) & (86400 < unix_current - df['ActionTime'] <= 259200), 'ActionID'])
                cnt_doc_5 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 5) & (86400 < unix_current - df['ActionTime'] <= 259200), 'ActionID'])
                cnt_doc_5_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 5) & (86400 < unix_current - df['ActionTime'] <= 259200), 'ActionID'])
            if atime > 259200: # больше 3 дней
                cnt_doc_3 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 3) & (unix_current - df['ActionTime'] > 259200), 'ActionID'])
                cnt_doc_3_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 3) & (unix_current - df['ActionTime'] > 259200), 'ActionID'])
                cnt_doc_4 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 4) & (unix_current - df['ActionTime'] > 259200), 'ActionID'])
                cnt_doc_4_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 4) & (unix_current - df['ActionTime'] > 259200), 'ActionID'])
                cnt_doc_5 = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['ActionID'] == 5) & (unix_current - df['ActionTime'] > 259200), 'ActionID'])
                cnt_doc_5_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & (df['ActionID'] == 5) & (unix_current - df['ActionTime'] > 259200), 'ActionID'])

            # Получаем кол-во дней, в которые клиент проявлял активность
            try:
                actions = len(df.loc[(df['ProfileID'] == tmp[0]) & ((df['ActionID'] == 0) | (df['ActionID'] == 4) | (df['ActionID'] == 5)), 'ActionTimeDay'])
                actions_t = len(df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & ((df['ActionID'] == 0) | (df['ActionID'] == 4) | (df['ActionID'] == 5)), 'ActionTimeDay'])
            except:
                actions = 0
                actions_t = 0

            # Получаем максимальное кол-во дней между активностями пользователя
            try:
                max_day = df.loc[(df['ProfileID'] == tmp[0]) & ((df['ActionID'] != 0) & (df['ActionID'] != 4) & (df['ActionID'] != 5)), 'ActionTimeDay'].max()
                min_day = df.loc[(df['ProfileID'] == tmp[0]) & ((df['ActionID'] != 0) & (df['ActionID'] != 4) & (df['ActionID'] != 5)), 'ActionTimeDay'].min()
                period = int((max_day - min_day).days)
                max_day_t = df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & ((df['ActionID'] != 0) & (df['ActionID'] != 4) & (df['ActionID'] != 5)), 'ActionTimeDay'].max()
                min_day_t = df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid) & ((df['ActionID'] != 0) & (df['ActionID'] != 4) & (df['ActionID'] != 5)), 'ActionTimeDay'].min()
                period_t = int((max_day_t - min_day_t).days)
            except:
                period = 0
                period_t = 0

            # Получаем кол-во дней между текущим временем и последним действием пользователя
            try:
                time_last = df.loc[(df['ProfileID'] == tmp[0]), 'ActionTimeDay'].max()
                allday = int((date_current - time_last).days)
                time_last_t = df.loc[(df['ProfileID'] == tmp[0]) & (df['SiteID'] == sid), 'ActionTimeDay'].max()
                if str(time_last_t) != 'NaT': allday_t = (date_current - time_last_t).days
                else: allday_t = 0
            except:
                allday = 0
                allday_t = 0


            # Обработка результатов:
            if tmp[1] == sid:   # Указанный при вводе сайт
                a.append(True)  # SiteTarget True

                # Delivery,Open,Click
                if atime <= 86400:
                    # Delivery-24,Open-24,Click-24
                    a.extend([cnt_doc_3_t, cnt_doc_4_t, cnt_doc_5_t, 0, 0, 0, 0, 0, 0])
                if 86400 < atime <= 259200:
                    # Delivery-3Day,Open-3Day,Click-3Day
                    a.extend([0, 0, 0, cnt_doc_3_t, cnt_doc_4_t, cnt_doc_5_t, 0, 0, 0])
                if atime > 259200:
                    # Delivery-3DMY,Open-3DMY,Click-3DMY
                    a.extend([0, 0, 0, 0, 0, 0, cnt_doc_3_t, cnt_doc_4_t, cnt_doc_5_t])

                # ActionDay
                if actions_t != '': a.append(actions_t)
                else: a.append(0)

                # PeriodDay
                if period_t != '': a.append(period_t)
                else: a.append(0)

                # Allday
                if allday_t != '': a.append(allday_t)
                else: a.append(0)

            else:               # Любой другой сайт
                a.append(False) # SiteTarget False

                # Delivery,Open,Click
                cnt_doc_3 = cnt_doc_3 - cnt_doc_3_t
                cnt_doc_4 = cnt_doc_4 - cnt_doc_4_t
                cnt_doc_5 = cnt_doc_5 - cnt_doc_5_t
                if atime <= 86400:
                    # Delivery-24,Open-24,Click-24
                    a.extend([cnt_doc_3, cnt_doc_4, cnt_doc_5, 0, 0, 0, 0, 0, 0])
                if 86400 < atime <= 259200:
                    # Delivery-3Day,Open-3Day,Click-3Day
                    a.extend([0, 0, 0, cnt_doc_3, cnt_doc_4, cnt_doc_5, 0, 0, 0])
                if atime > 259200:
                    # Delivery-3DMY,Open-3DMY,Click-3DMY
                    a.extend([0, 0, 0, 0, 0, 0, cnt_doc_3, cnt_doc_4, cnt_doc_5])

                # ActionDay
                actions = actions - actions_t
                if actions != '': a.append(actions)
                else: a.append(0)

                # PeriodDay
                period = period - period_t
                if period != '': a.append(period)
                else: a.append(0)

                # ActionDay
                allday = allday - allday_t
                if allday != '': a.append(abs(allday))
                else: a.append(0)

            features.append(a)

            # Контроль обработки
            if i == k:
                print('Обработано строк -', k)
                print('Время работы:  --- %s seconds ---' % (time.time() - unix_current))
                k += 100
                break

        except Exception:
            print('Неизвестная ошибка')
            continue



if __name__ == '__main__':
    # Загружаем датасет и добавляем вспомогательный признак
    df = pd.read_csv('raw_data.csv', sep=' ')
    df['ActionTimeDay'] = pd.to_datetime(df['ActionTime'], unit='s')

    # Запрашиваем ID сайта у пользователя
    print('*' * 70)
    print('Утилита предназначена для преобразования лога в датасет ML')
    print('*' * 70)
    print(df.head(5))
    print('*' * 70)
    print(df.info())
    print('*' * 70)
    site = input('Введите числовой идентификатор сайта, на котором клиент был активен,\n'
                 'либо получал письма или нажмите Enter для выхода из программы: ')
    print('*' * 70)

    if site.isdigit() == True: # Если ID указан верно

        # Получаем текущее время и дату
        unix_current = int(time.time())
        date_current = datetime.datetime.now()

        features = []
        sid = int(site)

        # Вызываем функцию преобразования лог-файла
        df_f = df_convert(df)

        # Многопоточность
        # thread1 = Thread(target=df_convert, args=(df,))
        # thread2 = Thread(target=df_convert, args=(df,))
        # thread1.start()
        # thread2.start()
        # thread1.join()
        # thread2.join()

        # Записываем обработанный датасет в файл
        df_f = pd.DataFrame(features, columns=['ProfileID','ActionTime','SiteTarget','Delivery-24','Open-24','Click-24',
                                               'Delivery-3Day','Open-3Day','Click-3Day','Delivery-3DMY','Open-3DMY',
                                               'Click-3DMY','ActionDay','PeriodDay','AllDay'])
        df_f.to_csv('dataset_logs.csv', sep = ';', index = False)

    else:
        print('Неверный ввод или завершение работы')

    print('Обработка завершена, время работы:  --- %s seconds ---' % (time.time() - unix_current))