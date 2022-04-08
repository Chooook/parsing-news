import os
import re
import sys
import requests

import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tqdm import tqdm

from config import *


def connection_check(session):
    try:
        session.get('https://yandex.ru/')
    except requests.exceptions.HTTPError as http_err:
        print(f'Проблемы с интернет подключением: {http_err}')
        sys.exit()
    except Exception as err:
        print(f'Ошибка: {err}')
        sys.exit()


def files_parse():
    try:
        if os.path.exists(input_dir):
            files = []
            for _, _, data in os.walk(input_dir):
                for file in reversed(data):
                    if re.search(r'.csv', file):
                        files.append(file)
            if not files:
                raise IOError
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            return files
        else:
            os.makedirs(input_dir)
            raise IOError
    except IOError:
        print('Отсутствуют файлы с ключевыми словами\nНеобходимо использовать файлы формата .csv')
        sys.exit()
    except Exception as err:
        print(f'Ошибка: {err}')


def read_keys(files):
    keys = []
    for filename in files:
        key_words = pd.read_csv(input_dir + filename, header=None)[0].tolist()
        keys += key_words
    return keys


def links_parser(session, keys, engine, morph):

    def normal_form_str(not_normal):
        normal = []
        not_normal = ''.join(re.findall(r'\w|\s', not_normal))
        for word in not_normal.split():
            normal_word = morph.parse(word)[0].normal_form
            normal.append(normal_word)
        normal = ' '.join(normal)
        return normal

    for key in tqdm(keys):
        try:
            qs, qe = engines[engine]
            response = session.get(
                qs
                + key
                + qe
            )
        except requests.exceptions.HTTPError as http_err:
            print(f'Запрос "{key}" не выполнен! Проблемы с интернет подключением: {http_err}')
        except Exception as err:
            print(f'Запрос "{key}" не выполнен! Ошибка: {err}')
        else:
            soup = BeautifulSoup(response.text, 'lxml')

            if engine == 'yandex':
                for el in soup.findAll({'a': True}, class_='mg-snippet__url'):
                    link = el['href']
                    if link.find('.ua/') == -1 and link.find('.uz/') == -1:
                        tail = link[link.find('utm') - 1:]
                        link = link.replace(tail, '')
                        title = el.div.span.text
                        flag = False
                        if titles:
                            for elem in titles:
                                # if fuzz.token_sort_ratio(elem.lower(), title.lower()) >= 60:
                                if fuzz.token_sort_ratio(normal_form_str(elem),
                                                         normal_form_str(title)) >= 60:
                                    flag = True
                        else:
                            links.append(link)
                            titles.append(title)
                            keys_for_table.append(key)
                        if not flag:
                            links.append(link)
                            titles.append(title)
                            keys_for_table.append(key)
                            # print(title)

            else:
                for el in soup.findAll({'a': True}, class_='title'):
                    link = el['href']
                    if link.find('.ua/') == -1 and link.find('.uz/') == -1:
                        title = el.text
                        flag = False
                        if titles:
                            for elem in titles:
                                # if fuzz.token_sort_ratio(elem.lower(), title.lower()) >= 60:
                                if fuzz.token_sort_ratio(normal_form_str(elem),
                                                         normal_form_str(title)) >= 60:
                                    flag = True
                        else:
                            links.append(link)
                            titles.append(title)
                            keys_for_table.append(key)
                        if not flag:
                            links.append(link)
                            titles.append(title)
                            keys_for_table.append(key)

    return links, titles, keys_for_table


# def drop_by_titles(df):
#     to_drop = set()
#     titles_to_compare = df['Заголовок'].tolist()
#     for row in tqdm(df.index):
#         title = df.loc[row, 'Заголовок']
#         for i, elem in enumerate(titles):
#             print(elem, title)
#             if fuzz.token_sort_ratio(elem, title) >= 60:
#                 print(fuzz.token_sort_ratio(elem, title))
#                 del titles[i]
#                 to_drop.add(row)
#                 break
#     df.drop(to_drop, inplace=True)
#     return df


def text_parser(df, session):
    parsed_links = df['Ссылки'].tolist()
    texts = []
    if parsed_links:
        for link in tqdm(parsed_links):
            try:
                response = session.get(link)
            except requests.exceptions.HTTPError as http_err:
                print(f'Не удалось выгрузить данные с {link}!!! Проблемы с интернет подключением: {http_err}')
                texts.append(f'Не удалось выгрузить данные!!! Проблемы с интернет подключением: {http_err}')
            except Exception as err:
                print(f'Не удалось выгрузить данные с {link}!!! Ошибка: {err}')
                texts.append(f'Не удалось выгрузить данные!!! Ошибка: {err}')
            else:
                response.encoding = 'utf8'
                soup = BeautifulSoup(response.text, 'lxml')
                text = []
                for div in soup.findAll({'div': True}):
                    # if div.find({'div': True}):
                    #     continue
                    if div.find({'p': True}):
                        for el in div.findAll({'p': True,
                                               'li': True,
                                               # 'div': True
                                               }):
                            while el.text not in text:
                                text.append(el.text)
                # text = soup.get_text(separator='\n', strip=True).split('\n')
                correct_text = []
                for par in text:
                    # if par.strip():
                    if par.strip() and len(par.split(' ')) >= 2 and par.strip()[-1] in '!;?:,.;':
                        correct_text.append(par)
                correct_text = '\n'.join(correct_text)
                texts.append(correct_text)
    else:
        print('Список ссылок пуст!')
    return texts


def clear_texts(df):
    for row in df.index:
        df.loc[row, 'Тексты'] = re.sub(r'\n{2,}', r'\n', df.loc[row, 'Тексты'])
    to_drop = set()
    for row in df.index:
        text = df.loc[row, 'Тексты']
        if pd.notna(text):
            key = df.loc[row, 'Ключевое слово']
            if not re.search(rf'\b{key.lower()}\b', text.lower()):
                to_drop.add(row)
        else:
            to_drop.add(row)
    df.drop(to_drop, inplace=True)

    return df

# new_keys = ['аудит', 'аудитор', 'аудиторский']
# morph = pymorphy2.MorphAnalyzer()
# keys = []
# for i in new_keys:
#     words = morph.parse(i)[0].lexeme
#     for j in words:
#         keys.append(j[0])
# keys = list(set(keys))
# print(keys)
