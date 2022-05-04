import os
import re
import sys

import pandas as pd

from _config import Dirs


class GetData:
    @staticmethod
    def makedirs():
        for directory in Dirs.items:
            if not os.path.exists(directory):
                os.makedirs(directory)

    @staticmethod
    def search_files(directory) -> set[str]:
        try:
            files = set(next(os.walk(directory))[2])
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
    def read_keys():
        directory = Dirs.input_dir
        files = GetData.search_files(directory)
        if not files:
            print(f'Папка с ключевыми словами, "{directory}", пуста!')
            sys.exit()
        keys = set()
        for filename in files:
            if not filename.endswith('.csv'):
                print(f'Файл "{directory + filename}" не подходит.\n'
                      'Необходимо использовать файлы формата .csv')
            else:
                try:
                    file_keys = set()
                    # os.path.join(directory, filename)
                    with open(directory + filename) as file:
                        for row in file:
                            file_keys.add(re.sub(r'\n\r', '', row).strip())
                    file.close()
                    keys = keys.union(file_keys)
                # TOD to fix
                except pd.errors.EmptyDataError:
                    print(f'Файл "{filename}" пуст!\n'
                          'Ключевые слова необходимо '
                          'расположить в первой колонке.')

        keys = set([key.lower() for key in keys])
        if not keys:
            print('Ключевых слов не обнаружено.\n'
                  'Необходимо использовать файлы формата '
                  '.csv со словами в первой колонке.')
            sys.exit()
        else:
            print(f'Обнаружено ключевых слов: {len(keys)}')
            return keys

    @staticmethod
    def read_stopwords():
        directory = Dirs.stopwords_dir
        stopwords = set()
        files = GetData.search_files(directory)
        if not files:
            print(f'Папка со стоп-словами, "{directory}", пуста!\n'
                  'Похожие заголовки будут определяться менее точно.')
            return stopwords
        for filename in files:
            if filename[-4:] == '.csv':
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
