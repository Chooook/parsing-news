import os
import re
import time
from datetime import datetime

import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm

from _config import Paths, Times
from _utils import Utils


class History:
    df = pd.DataFrame()

    # TODO: очистить историю от иностранных заголовков?
    #  путем замены на пустую строку
    @classmethod
    def check(cls, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        cls.df = df
        old_links, old_titles = History.__read()
        cls.df.drop(History.__links_check(old_links), inplace=True)
        _history_df = cls.df.loc[:, ['Заголовок', 'Ссылка']]
        print(f'Новых строк для обновления истории: {len(cls.df.index)}')
        if old_titles and not cls.df.empty:
            print(f'Строк в истории: {len(old_titles)}'
                  # 'Проверка займет до'
                  # f' {round(len(old_titles)*len(df.index)*0.003)}'
                  # ' секунд'
                  )
        time.sleep(0.1)  # для корректного вывода tqdm в консоли PyCharm
        if old_titles:
            cls.df.drop(History.__titles_check(old_titles),
                        inplace=True
                        )
        _history_df['Заголовок'] = [
            Utils.normal_str(title) for title
            in _history_df['Заголовок'].values
        ]
        return cls.df, _history_df

    @staticmethod
    def __read() -> tuple[set[str], set[str]]:
        """Return set of links and set of titles
        contained in .csv files from history_path dir.

        :rtype: tuple[set[str], set[str]]
        :return: set of links and set of titles
        """
        delta = Times.history_delta
        replace_delta = Times.replace_delta
        path = Paths.history_path
        old_history_path = Paths.old_history_path
        files = Utils.search_files(path)
        if not files:
            print('История пуста.')
            return set(), set()
        __old_links, __old_titles = set(), set()
        to_replace = set()
        for filename in files:
            if not filename.endswith('.csv'):
                print(f'Файл "{path + filename}" не подходит.\n'
                      'Необходимо использовать файлы формата .csv')
            else:
                filedate_str = ''.join(re.findall(r'\d', filename))
                try:
                    filedate = datetime.strptime(filedate_str, '%Y%m%d%H%M%S')
                except ValueError:
                    continue
                if filedate + replace_delta <= Times.today:
                    to_replace.add(filename)

                elif filedate + delta >= Times.today:
                    try:
                        hist_file = pd.read_csv(path + filename, sep=';')
                        __old_links = __old_links.union(
                            set(hist_file['Ссылка'].tolist())
                        )
                        __old_titles = __old_titles.union(
                            set(hist_file['Заголовок'].tolist())
                        )
                    except pd.errors.EmptyDataError:
                        print(f'В файле "{filename}" нет истории.')
                        continue

        for file in to_replace:
            os.replace(path + file, old_history_path + file)
        if not __old_links and not __old_titles:
            print('История пуста.')
        return __old_links, __old_titles

    @classmethod
    def __links_check(cls, old_l: set[str]) -> set[int]:
        """Return set of rows, provided that their links has
         a duplicate among titles contained in history.

        :param old_l: links contained in history
        :type old_l: set[str]

        :rtype: set[int]
        :return: set of rows to drop them from df
        """
        to_drop = set()
        for index, link in zip(cls.df.index, cls.df['Ссылка'].values):
            if link in old_l:
                to_drop.add(index)
        return to_drop

    @classmethod
    def __titles_check(cls, old_t: set[str]) -> set[int]:
        """Return set of rows, provided that their titles
         is similar to at least one of titles contained in history.

        :param old_t: titles contained in history
        :type old_t: set[str]

        :rtype: set[int]
        :return: set of rows to drop them from df
        """
        to_drop = set()
        for index, title in tqdm(
                list(zip(cls.df.index, cls.df['Заголовок'].values)),
                desc='Очистка по похожим заголовкам из истории'
        ):
            for old_title in old_t:
                nf_title = Utils.normal_str(title)
                if fuzz.token_sort_ratio(
                        nf_title,
                        old_title) >= 60:
                    to_drop.add(index)
                    break
        return to_drop
