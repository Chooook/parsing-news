import re
import sys
import time

import pandas as pd
import pymorphy2
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tqdm import tqdm

from _config import Dirs, Times, Engine, Ya, Bing
from _getdata import GetData
from _history import History
from _textparser import TextParser


class Connection:
    @staticmethod
    def session_create() -> requests.sessions.Session:
        s = requests.Session()
        Connection.check_connection(s)
        return s

    @staticmethod
    def check_connection(session: requests.sessions.Session):
        try:
            session.get('https://yandex.ru/', timeout=25)

        except IOError as http_err:
            print(f'Проблемы с интернет подключением!')
            sys.exit()

        except Exception as err:
            print(f'Ошибка:\n'
                  f'{err}')
            sys.exit()


class MainParser:
    @staticmethod
    def normal_form_str(not_normal: str, morph, stopwords) -> str:
        normal = []
        not_normal = re.sub(r'[^\w\s]', '', not_normal)
        for word in not_normal.split():
            if word not in stopwords:
                normal_word = morph.parse(word)[0].normal_form
                normal.append(normal_word)
        normal = ' '.join(normal)
        return normal

    @staticmethod
    def __links_and_titles(k: str, eng, q: str, session, cls):
        try:
            response = session.get(q, timeout=10)

        except IOError as http_err:
            print(f'Запрос "{k}" не выполнен! '
                  f'Проблемы с интернет подключением:\n'
                  f'{http_err}')
            Connection.check_connection(session)
            return [], [], []

        # сделать перезапуск итерации при ошибке
        # выполнения запроса и ограничение по количеству попыток
        except Exception as err:
            print(f'Запрос "{k}" не выполнен! Ошибка:\n'
                  f'{err}')
            Connection.check_connection(session)
            return [], [], []

        else:
            keys_for_table, links, titles = [], [], []
            soup = BeautifulSoup(response.text, 'lxml')
            for el in soup.findAll({'a': True}, class_=cls):
                link = el['href']
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

    @staticmethod
    def __titles_check(df, morph, stopwords):
        to_drop = set()
        titles_to_compare = df['Заголовок']
        for index, title_one in tqdm(list(zip(df.index, titles_to_compare)), desc='Очистка по похожим заголовкам'):
            for i, title_two in enumerate(titles_to_compare):
                if i not in to_drop and i != index:
                    if fuzz.token_sort_ratio(
                            MainParser.normal_form_str(title_one, morph, stopwords),
                            MainParser.normal_form_str(title_two, morph, stopwords)) >= 60:
                        to_drop.add(index)
                        break
        return to_drop

    @staticmethod
    def get_articles():
        GetData.makedirs()

        keys_for_table, links, titles = [], [], []

        session = Connection.session_create()
        morph = pymorphy2.MorphAnalyzer()
        keys = GetData.read_keys()
        stopwords = GetData.read_stopwords()

        for engine in Engine.registry:
            qs = engine.query_start
            qe = engine.query_end_day
            cls = engine.article_class
            for key in tqdm(keys, desc=f'Выгрузка ссылок из {engine.name}'):
                query = qs + key + qe
                args = (key, engine, query, session, cls)
                k, l, t = MainParser.__links_and_titles(*args)
                keys_for_table += k
                links += l
                titles += t

        result = pd.DataFrame({
            'Ключевое слово': keys_for_table,
            'Заголовок': titles,
            'Ссылка': links
        })
        result.drop_duplicates(['Ссылка'], inplace=True, ignore_index=True)
        result, history_df = History.history_check(result, MainParser.normal_form_str, morph, stopwords)
        time.sleep(0.1)
        result.drop(MainParser.__titles_check(result, morph, stopwords), inplace=True)
        result = TextParser.text_parser(result, session)
        session.close()

        result.to_excel(
            Dirs.output_dir + f'result_{Times.launch_time_str}.xlsx',
            index=False
        )
        history_df.to_csv(
            Dirs.history_dir + f'history_{Times.launch_time_str}.csv',
            sep=';',
            index=False
        )


if __name__ == '__main__':
    MainParser.get_articles()
