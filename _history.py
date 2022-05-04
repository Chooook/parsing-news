import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm

from _config import Dirs, Times
from _getdata import GetData


class History:
    @staticmethod
    def __read_history(delta: timedelta) -> tuple[set[str], set[str]]:
        directory = Dirs.history_dir
        old_dir = Dirs.old_history_dir
        files = GetData.search_files(directory)
        if not files:
            print('История пуста.')
            return set(), set()
        __old_links, __old_titles = set(), set()
        to_replace = set()
        for filename in files:
            if filename[-4:] == '.csv':
                filedate_str = ''.join(re.findall(r'\d', filename))
                try:
                    filedate = datetime.strptime(filedate_str, '%Y%m%d%H%M%S')
                except ValueError:
                    continue
                if filedate + delta >= Times.today:
                    try:
                        hist_file = pd.read_csv(directory + filename, sep=';')
                        __old_links = __old_links.union(set(hist_file['Ссылка'].tolist()))
                        __old_titles = __old_titles.union(set(hist_file['Заголовок'].tolist()))
                    except pd.errors.EmptyDataError:
                        print(f'В файле "{filename}" нет истории.')
                        continue
                else:
                    to_replace.add(filename)
            else:
                print(f'Файл "{directory + filename}" не подходит.\n'
                      'Необходимо использовать файлы формата .csv')
        for file in to_replace:
            os.replace(directory + file, old_dir + file)
        if not __old_links and not __old_titles:
            print('История пуста.')
        return __old_links, __old_titles

    @staticmethod
    def __history_links_check(df, old_l):
        to_drop = set()
        for index, link in zip(df.index, df['Ссылка'].values):
            if link in old_l:
                to_drop.add(index)
        return to_drop

    @staticmethod
    def __history_titles_check(df, func, old_t, morph, stopwords):
        # new_titles = df['Заголовок']
        to_drop = set()
        for index, title in tqdm(list(zip(df.index, df['Заголовок'].values)), desc='Очистка по похожим заголовкам из истории'):
            for old_title in old_t:
                if fuzz.token_sort_ratio(
                        func(title, morph, stopwords),
                        func(old_title, morph, stopwords)) >= 60:
                    to_drop.add(index)
                    break
        return to_drop

    @staticmethod
    def history_check(df, nf_func, morph, stopwords):
        old_links, old_titles = History.__read_history(Times.history_timedelta)
        df.drop(History.__history_links_check(df, old_links), inplace=True)
        _history_df = df.loc[:, ['Заголовок', 'Ссылка']]
        print(f'Новых строк для обновления истории: {len(df.index)}')
        time.sleep(0.1)
        df.drop(History.__history_titles_check(df, nf_func, old_titles, morph, stopwords), inplace=True)
        return df, _history_df
