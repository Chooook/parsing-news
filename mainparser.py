import re
import time

import pandas as pd
import pycld2 as cld
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tqdm import tqdm

from _config import Paths, Engine, Times
from _history import History
from _textparser import TextParser
from _utils import Utils


class MainParser:
    __session = None
    __keys = None
    __df = pd.DataFrame()

    @classmethod
    def get_articles(cls):
        Utils.makedirs()
        cls.__session = Utils.create_session()
        cls.__keys = Utils.read_keys()

        keys_for_table, links, titles = [], [], []
        search_links = []

        for engine in Engine.__subclasses__():
            qs = engine.query_start
            qe = engine.query_end_day
            art_cls = engine.article_class
            for key in tqdm(cls.__keys,
                            desc=f'Выгрузка ссылок из {engine.name}'
                            ):
                query = qs + key + qe
                k, l, t = MainParser.__links_and_titles(key, query,
                                                        art_cls, engine)
                keys_for_table += k
                links += l
                titles += t
                search_links += [query for _ in range(len(k))]

        cls.__session.close()

        cls.__df = pd.DataFrame({
            'Ключевое слово': keys_for_table,
            'Заголовок': titles,
            'Ссылка': links,
            'Поисковой url': search_links
        })
        cls.__df.drop_duplicates(['Ссылка'], inplace=True, ignore_index=True)
        cls.__df, history_df = History.check(cls.__df)
        time.sleep(0.1)  # для корректного вывода tqdm в консоли PyCharm

        cls.__df.drop(MainParser.__titles_check(), inplace=True)
        cls.__df = TextParser.parser(cls.__df)
        df_ending, to_drop = MainParser.__replace_rows()
        cls.__df.drop(to_drop, inplace=True)
        MainParser.__drop_rows()
        cls.__df = pd.concat([cls.__df, df_ending], ignore_index=True)

        cls.__df.to_excel(Paths.output_path +
                          # f'result_{Paths.keys_dir_name}'
                          f'result'
                          f'_{Times.launch_time_str}.xlsx',
                          index=False
                          )
        history_df.to_csv(Paths.history_path +
                          f'history_{Times.launch_time_str}.csv',
                          sep=';',
                          index=False
                          )

    @classmethod
    def __links_and_titles(cls, key: str, q: str, art_cls: str,
                           eng: Engine.__subclasses__()
                           ) -> tuple[list[str], list[str], list[str]]:
        """Return lists of keys, links and titles for search query.

        :param key: keyword for search
        :type key: str
        :param q: search link
        :type q: str
        :param art_cls: html class with title and link
        :type art_cls: str
        :param eng: search engine
        :type eng: Engine.__subclass__

        :rtype: tuple[list[str], list[str], list[str]]
        :return: 3 lists with keys, links and titles for search query
        """
        try:
            response = cls.__session.get(q, timeout=10)

        except IOError as http_err:
            print(f'Запрос "{key}" не выполнен! '
                  f'Проблемы с интернет подключением:\n'
                  f'{http_err}')
            Utils.check_connection(cls.__session)
            return [], [], []

        # TODO: сделать перезапуск итерации при ошибке выполнения запроса
        #  и ограничение по количеству попыток
        except Exception as err:
            print(f'Запрос "{key}" не выполнен! Ошибка:\n'
                  f'{err}')
            Utils.check_connection(cls.__session)
            return [], [], []

        else:
            keys_for_table, links, titles = [], [], []
            soup = BeautifulSoup(response.text, 'lxml')
            for el in soup.findAll({'a': True}, class_=art_cls):
                link, title = eng.get_article(eng, el)
                keys_for_table.append(key)
                links.append(link)
                titles.append(title)
        return keys_for_table, links, titles

    @classmethod
    def __titles_check(cls) -> set[int]:
        """Return set of rows, provided that their titles
         is similar to at least one of other titles.

        :rtype: set[int]
        :return: set of rows to drop them from df
        """
        to_drop = set()
        cls.__df.reset_index(inplace=True, drop=True)
        normal_titles = [Utils.normal_str(t) for t
                         in cls.__df['Заголовок'].values]
        for ind_one, title_one in enumerate(normal_titles):
            for ind_two, title_two in enumerate(normal_titles):
                if ind_two not in to_drop and ind_two != ind_one:
                    if fuzz.token_sort_ratio(title_one, title_two) >= 60:
                        to_drop.add(ind_one)
                        break

        return to_drop

    @classmethod
    def __replace_rows(cls) -> tuple[pd.DataFrame, set[int]]:
        """Return pd.Dataframe with articles to add it to the end of
        result df and set of ints to drop them from result df.

        :rtype: tuple[pd.Dataframe, set[int]]
        :return: pd.Dataframe with articles and set of ints-indexes
        """
        df_ending = pd.DataFrame()

        to_end = set()
        for row, text in zip(cls.__df.index, cls.__df['Текст'].values):
            if (text == 'В тексте отсутствуют русские символы!' or
                    re.search(r'Не удалось выгрузить данные!!!', text)):
                to_end.add(row)
        df_ending = pd.concat([df_ending, cls.__df.loc[list(to_end)]],
                              ignore_index=True)

        return df_ending, to_end

    @classmethod
    def __drop_rows(cls):
        """Drop articles from result df:
        with "schroders" in url;
        without keyword in text;
        with non-russian text.
        """
        to_drop = set()
        for row, link in zip(cls.__df.index, cls.__df['Ссылка'].values):
            if re.search('schroders', link):
                to_drop.add(row)
        cls.__df.drop(to_drop, inplace=True)

        to_drop = set()
        for row, text, title, key in zip(
                cls.__df.index,
                cls.__df['Текст'].values,
                cls.__df['Заголовок'].values,
                cls.__df['Ключевое слово'].values
        ):
            if not len(key.split(',')) > 1:
                if pd.notna(text):
                    key_clear = re.sub(r'"', '', key)
                    if (not re.search(rf'\b{key_clear}\b', text.lower()) and
                            not re.search(rf'\b{key_clear}\b', title.lower())):
                        to_drop.add(row)
                else:
                    to_drop.add(row)
        cls.__df.drop(to_drop, inplace=True)

        to_drop = set()
        for row, text in zip(cls.__df.index, cls.__df['Текст'].values):
            try:
                if cld.detect(text)[2][0][0] != 'RUSSIAN':
                    to_drop.add(row)
            except cld.error:
                to_drop.add(row)
        cls.__df.drop(to_drop, inplace=True)


if __name__ == '__main__':
    # start_time = time.time()
    MainParser.get_articles()
    # print("%s секунд" % (time.time() - start_time))
