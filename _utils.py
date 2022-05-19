import os
import re
import sys
from pathlib import Path

import pandas as pd
import pymorphy2
import requests

from _config import Paths


class Utils:
    __stopwords = None
    __morph = None

    @staticmethod
    def makedirs():
        """Create necessary directories listed in _config.Paths"""
        for path in Paths.items:
            if not os.path.exists(path):
                os.makedirs(path)

    @staticmethod
    def search_files(path: str) -> set[str]:
        """Return filenames found in 'path' directory.

        :param path: directory to search for files
        :type path: str

        :rtype: set[str]
        :return: set of discovered 'filename.extension' strings
        """
        # path = Path(directory)
        try:
            files = set(next(os.walk(path))[2])
            try:
                files.remove('.DS_Store')
            except KeyError:
                pass
            return files
        except Exception as err:
            print('Ошибка при поиске файлов:\n'
                  f'{err}')
            sys.exit()

    @staticmethod
    def __read_files(path: str) -> set[str]:
        """Return lowercase stripped rows
        contained in .csv files from 'path'

        :param path:
        :type path: str

        :rtype: set[str]
        :return: set of rows
        """
        files = Utils.search_files(path)
        if not files:
            print(f'Папка, "{path}", пуста!')
            return set()
        rows = set()
        for filename in files:
            file = Path(path + filename)
            if not filename.endswith('.csv'):
                print(f'Файл "{file}" не подходит.\n'
                      'Необходимо использовать файлы формата .csv')
            elif file.stat().st_size == 0:
                print(f'Файл "{filename}" пуст!')
            else:
                file_rows = set()
                with open(file, encoding='utf-8-sig') as csv:
                    for row in csv:
                        file_rows.add(row.strip().lower())
                csv.close()
                rows = rows.union(file_rows)
        return rows

    @staticmethod
    def read_keys() -> set[str]:
        """Return keywords
        contained in .csv files from keys_path dir:
        keys-word forms generated from a single word;
        keys-phrases.

        :rtype: set[str]
        :return: set of keywords
        """
        path = Paths.keys_path
        files = Utils.search_files(path)
        if not files:
            print(f'Папка с ключевыми словами, "{path}", пуста!')
            sys.exit()
        keys = set()
        for filename in files:
            if not filename.endswith('.csv'):
                print(f'Файл "{path + filename}" не подходит.\n'
                      'Необходимо использовать файлы формата .csv')
            elif Path(path + filename).stat().st_size == 0:
                print(f'Файл "{filename}" пуст!\n'
                      'Ключевые слова необходимо '
                      'расположить в первой колонке.')
            else:
                file_keys = set()
                # os.path.join(directory, filename)
                with open(path + filename, encoding='utf-8-sig') as file:
                    for row in file:
                        key = ''.join(re.findall(r'[\w+ \"\']', row))
                        key = re.sub(r' +', ' ', key)
                        if key:
                            file_keys.add(key.strip().lower())
                file.close()
                keys = keys.union(file_keys)

        if not keys:
            print('Ключевых слов не обнаружено.\n'
                  'Необходимо использовать файлы формата '
                  '.csv со словами в первой колонке.')
            sys.exit()
        else:
            new_keys = set()
            for key in keys:
                new_keys = new_keys.union(Utils.__word_forms(key))
            print(f'Обнаружено ключевых слов: {len(new_keys)}')
            return new_keys

    @staticmethod
    def __read_stopwords() -> set[str]:
        """Return stopwords contained in
         .csv files from stopwords_path dir.

        :rtype: set[str]
        :return: set of stopwords
        """
        directory = Paths.stopwords_path
        stopwords = set()
        files = Utils.search_files(directory)
        if not files:
            print(f'Папка со стоп-словами, "{directory}", пуста!\n'
                  'Похожие заголовки будут определяться менее точно.')
            return stopwords
        for filename in files:
            if filename.endswith('.csv'):
                try:
                    stopwords = stopwords.union(
                        set(pd.read_csv(
                            directory + filename,
                            header=None
                        )[0].tolist()))
                except pd.errors.EmptyDataError:
                    print(f'Файл "{filename}" пуст!\n'
                          'Стоп-слова необходимо расположить '
                          'в первой колонке.')
                    continue
            else:
                print(f'Файл "{directory + filename}" не подходит.\n'
                      'Необходимо использовать файлы формата .csv')

        if not stopwords:
            print(
                'Стоп-слов не обнаружено! '
                'Необходимо использовать файлы формата '
                '.csv со словами в первой колонке.\n'
                'Похожие заголовки будут определяться менее точно.')
        return stopwords

    @staticmethod
    def create_session() -> requests.sessions.Session:
        """Return requests.Session() object and check connection."""
        s = requests.Session()
        Utils.check_connection(s)
        return s

    @staticmethod
    def check_connection(session: requests.sessions.Session):
        """Check internet connection.

        :param session: session object to be checked
        :type session: requests.sessions.Session
        """
        try:
            session.get('https://yandex.ru/', timeout=25)

        except IOError:
            print(f'Проблемы с интернет подключением!')
            sys.exit()

        except Exception as err:
            print(f'Ошибка:\n'
                  f'{err}')
            sys.exit()

    @classmethod
    def normal_str(cls, not_normal: str) -> str:
        """Normalise string.

        :param not_normal: not normalised string
        :type not_normal: str

        :rtype: str
        :return: normalised string
        """
        if not cls.__stopwords:
            cls.__stopwords = Utils.__read_stopwords()
        normal = []
        not_normal = re.sub(r'[^\w\s]', '', not_normal)
        for word in not_normal.split():
            if word not in cls.__stopwords:
                normal_word = cls.__morph.parse(word)[0].normal_form
                normal.append(normal_word)
        normal = ' '.join(normal)
        return normal

    @classmethod
    def __word_forms(cls, word: str) -> set[str]:
        """Return word forms for single-word keywords and phrases.

        :param word: set of words to create word forms
        :type word: str

        :rtype: set[str]
        :return: phrases or word forms generated from a single words
        """
        if not cls.__morph:
            cls.__morph = pymorphy2.MorphAnalyzer()
        new_words = set()
        clean_word = ''.join(re.findall(r'[\w+ ]', word))
        if len(clean_word.split()) > 1:
            new_words.add(clean_word)
        elif clean_word:
            forms = cls.__morph.parse(clean_word)[0].lexeme
            for form in forms:
                new_words.add('"' + form[0] + '"')

        return new_words

# class Article:
#     def __init__(self, name, key, link, title):
#         self.name = name
#         self.key = key
#         self.link = link
#         self.title = title
#         self.text = None
