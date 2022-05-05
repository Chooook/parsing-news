import re
import time

import pandas as pd
import pymorphy2
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tqdm import tqdm

from _config import Connection, Dirs, Times, Engine, Bing, Ya
from _getdata import GetData
from _history import History
from _textparser import TextParser


class MainParser:
    __session = Connection.session_create()
    __morph = pymorphy2.MorphAnalyzer()
    __keys = GetData.keys()
    __stopwords = GetData.stopwords()

    @classmethod
    def get_articles(cls):
        GetData.makedirs()

        keys_for_table, links, titles = [], [], []

        for engine in Engine.registry:
            qs = engine.query_start
            qe = engine.query_end_day
            art_cls = engine.article_class
            for key in tqdm(cls.__keys,
                            desc=f'Выгрузка ссылок из {engine.name}'
                            ):
                query = qs + key + qe
                k, l, t = MainParser.__links_and_titles(
                    key,
                    engine,
                    query,
                    art_cls
                    )
                keys_for_table += k
                links += l
                titles += t

        cls.__session.close()

        result = pd.DataFrame({
            'Ключевое слово': keys_for_table,
            'Заголовок': titles,
            'Ссылка': links
        })
        result.drop_duplicates(['Ссылка'], inplace=True, ignore_index=True)
        result, history_df = History.check(result,
                                           MainParser.normal_str
                                           )
        time.sleep(0.1)
        result.drop(MainParser.__titles_check(result), inplace=True)
        result = TextParser.parser(result)

        result.to_excel(
            Dirs.output_dir + f'result_{Times.launch_time_str}.xlsx',
            index=False
        )
        history_df.to_csv(
            Dirs.history_dir + f'history_{Times.launch_time_str}.csv',
            sep=';',
            index=False
        )

    @classmethod
    def normal_str(cls, not_normal: str) -> str:
        normal = []
        not_normal = re.sub(r'[^\w\s]', '', not_normal)
        for word in not_normal.split():
            if word not in cls.__stopwords:
                normal_word = cls.__morph.parse(word)[0].normal_form
                normal.append(normal_word)
        normal = ' '.join(normal)
        return normal

    @classmethod
    def __links_and_titles(cls, k: str, eng, q: str, art_cls):
        try:
            response = cls.__session.get(q, timeout=10)

        except IOError as http_err:
            print(f'Запрос "{k}" не выполнен! '
                  f'Проблемы с интернет подключением:\n'
                  f'{http_err}')
            Connection.check_connection(cls.__session)
            return [], [], []

        # сделать перезапуск итерации при ошибке
        # выполнения запроса и ограничение по количеству попыток
        except Exception as err:
            print(f'Запрос "{k}" не выполнен! Ошибка:\n'
                  f'{err}')
            Connection.check_connection(cls.__session)
            return [], [], []

        else:
            keys_for_table, links, titles = [], [], []
            soup = BeautifulSoup(response.text, 'lxml')
            for el in soup.findAll({'a': True}, class_=art_cls):
                link = el['href']
                # изменить на match-case
                if eng is Ya:
                    tail = link[link.find('utm') - 1:]
                    link = link.replace(tail, '')
                    title = el.div.span.text
                elif eng is Bing:
                    title = el.text
                else:
                    # print(f'Нет параметров для поисковика {engine}')
                    break
                keys_for_table.append(k)
                links.append(link)
                titles.append(title)
        return keys_for_table, links, titles

    @classmethod
    def __titles_check(cls, df):
        to_drop = set()
        titles_to_compare = df['Заголовок']
        for index, title_one in tqdm(list(zip(df.index, titles_to_compare)),
                                     desc='Очистка по похожим заголовкам'
                                     ):
            for i, title_two in enumerate(titles_to_compare):
                if i not in to_drop and i != index:
                    if fuzz.token_sort_ratio(
                            MainParser.normal_str(title_one),
                            MainParser.normal_str(title_two)) >= 60:
                        to_drop.add(index)
                        break
        return to_drop


if __name__ == '__main__':
    MainParser.get_articles()
