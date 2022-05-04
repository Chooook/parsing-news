import re
import sys

import chardet
import pandas as pd
import pycld2 as cld2
from bs4 import BeautifulSoup
from tqdm import tqdm


class TextParser:

    @staticmethod
    def text_parser(df, session):

        if df['Ссылка'].empty:
            print('Список ссылок пуст!')
            sys.exit()
        texts = []
        charsets = {'utf-8', 'windows-1251', 'iso-8859-5'}
        for link, key in tqdm(list(zip(
                df['Ссылка'].values,
                df['Ключевое слово'].values
                )),
                desc='Выгрузка текстов'
                ):
            texts.append(TextParser.__get_text(key, session, link, charsets))

        df['Текст'] = texts
        df = TextParser.__replace_rows(df)

        return df

    @staticmethod
    def __get_text(key, session, link, charsets):
        try:
            response = session.get(link, timeout=15)
        except IOError as http_err:
            print(f'Не удалось выгрузить данные с {link}!!!')
            return str(f'Не удалось выгрузить данные!!!'
                       f' Проблемы с подключением к ресурсу: {http_err}')
            # Connection.check_connection(session)
        except Exception as err:
            print(f'Не удалось выгрузить данные с {link}!!!')
            return f'Не удалось выгрузить данные!!! Ошибка: {err}'
            # Connection.check_connection(session)
        else:
            soup = BeautifulSoup(response.text, 'lxml')
            text = soup.get_text()
            if re.search(r'[А-Яа-я]', text):
                text = TextParser.__correct_text(soup, key)
                return text
            else:
                chardet_set = set()
                chardet_set.add(chardet.detect
                                (response.content).get('encoding'))
                for charset in charsets.union(chardet_set):
                    response.encoding = charset
                    soup = BeautifulSoup(response.text, 'lxml')
                    text = soup.get_text()
                    if re.search(r'[А-Яа-я]', text):
                        text = TextParser.__correct_text(soup, key)
                        return text
            return 'В тексте отсутствуют русские символы!'

    @staticmethod
    def __correct_text(bs, key):
        pretext = bs.get_text(separator='\n', strip=True).split('\n')
        corr_text = []
        key_clear = re.sub(r'"', '', key)
        for par in pretext:
            if ((par.strip() and len(par.split(' ')) >= 2 and
                 par.strip()[-1] in '!;?:,.') or
                    re.search(rf'\b{key_clear.lower()}\b', par.lower())):
                corr_text.append(par)
        corr_text = '\n'.join(corr_text)
        return corr_text

    @staticmethod
    def __drop_rows(df):

        to_drop = []
        for row, link in zip(df.index, df['Ссылка'].values):
            if re.search('schroders', link):
                to_drop.append(row)
        df.drop(to_drop, inplace=True)

        to_drop = []
        for row, text, title, key in zip(
                df.index,
                df['Текст'].values,
                df['Заголовок'].values,
                df['Ключевое слово'].values
        ):
            if pd.notna(text):
                key_clear = re.sub(r'"', '', key)
                if (not re.search(rf'\b{key_clear}\b', text.lower()) and
                        not re.search(rf'\b{key_clear}\b', title.lower())):
                    to_drop.append(row)
            else:
                to_drop.append(row)
        df.drop(to_drop, inplace=True)

        to_drop = []
        for row, text in zip(df.index, df['Текст'].values):
            if cld2.detect(text)[2][0][0] != 'RUSSIAN':
                to_drop.append(row)
        df.drop(to_drop, inplace=True)

        return df

    @staticmethod
    def __replace_rows(df):
        df_ending = pd.DataFrame()

        to_end = []
        for row, text in zip(df.index, df['Текст'].values):
            if text == 'В тексте отсутствуют русские символы!' or \
                    re.search(r'Не удалось выгрузить данные!!!', text):
                to_end.append(row)
        df_ending = pd.concat([df_ending, df.loc[to_end]], ignore_index=True)
        df.drop(to_end, inplace=True)

        df = TextParser.__drop_rows(df)

        df = pd.concat([df, df_ending], ignore_index=True)

        for row, text in zip(df.index, df['Текст'].values):
            df.loc[row, 'Текст'] = re.sub(r'\n{2,}', r'\n', text)

        return df


# class Article:
#     def __init__(self, name, key, link, title):
#         self.name = name
#         self.key = key
#         self.link = link
#         self.title = title
#         self.text = None

# import functools
# greet = functools.partial(greet, 'привет')
# greet('красавчик')

# lexemes:
# new_keys = ['аудит', 'аудитор', 'аудиторский']
# morph = pymorphy2.MorphAnalyzer()
# keys = []
# for i in new_keys:
#     words = morph.parse(i)[0].lexeme
#     for j in words:
#         keys.append(j[0])
# keys = list(set(keys))
# print(keys)
