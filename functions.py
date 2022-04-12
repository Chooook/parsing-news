import os
import re
import sys

import pandas as pd
import pycld2 as cld2
import pymorphy2
import chardet
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tqdm.auto import tqdm

from config import *


def connection_check(session):
    try:
        session.get('https://yandex.ru/', timeout=10)
    except IOError as http_err:
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
        sys.exit()


def read_keys(files):
    keys = []
    for filename in files:
        key_words = pd.read_csv(input_dir + filename, header=None)[0].tolist()
        keys += key_words
    return keys


def links_parser(session, keys):
    morph = pymorphy2.MorphAnalyzer()

    def normal_form_str(not_normal):
        normal = []
        not_normal = ''.join(re.findall(r'\w|\s', not_normal))
        for word in not_normal.split():
            normal_word = morph.parse(word)[0].normal_form
            normal.append(normal_word)
        normal = ' '.join(normal)
        return normal

    def history(df):
        df.drop_duplicates(['Ссылки'], inplace=True, ignore_index=True)

        if not os.path.isfile('history_file.csv'):
            history_df = df.loc[:, ['Ссылки', 'Заголовок']]
            history_df['Дата выгрузки'] = launch_time
            history_df.to_csv('history_file.csv', sep=';', index=False)
        else:
            to_drop = set()
            history_file = pd.read_csv('history_file.csv', sep=';')
            old_dates = set()
            for row in history_file.index:
                if history_file.loc[row, 'Дата выгрузки'] + datetime.timedelta(30) <= datetime.datetime.today():
                    old_dates.add(row)
            history_file.drop(old_dates, inplace=True)
            links_old = history_file['Ссылки']
            titles_old = history_file['Заголовок']
            for row in tqdm(df.index):
                new_link = df.loc[row, 'Ссылки']
                new_title = df.loc[row, 'Заголовок']
                if new_link in links_old:
                    to_drop.add(row)
                    continue
                for t in titles_old:
                    if fuzz.token_sort_ratio(normal_form_str(new_title), normal_form_str(t)) >= 60:
                        to_drop.add(row)
                        break
            df.drop(to_drop, inplace=True)
            history_df = df.loc[:, ['Ссылки', 'Заголовок']]
            history_df['Дата выгрузки'] = launch_time
            history_file = pd.concat([history_file, history_df], ignore_index=True)
            history_file.to_csv('history_file.csv', sep=';', index=False)
        return df

    for engine in engines.__iter__():
        for key in tqdm(keys):
            try:
                qs, qe = engines[engine]
                response = session.get(
                    qs
                    + key
                    + qe,
                    timeout=15
                )
            except IOError as http_err:
                print(f'Запрос "{key}" не выполнен! Проблемы с интернет подключением: {http_err}')
            except Exception as err:
                print(f'Запрос "{key}" не выполнен! Ошибка: {err}')
            else:
                soup = BeautifulSoup(response.text, 'lxml')

                if engine == 'yandex':
                    for el in soup.findAll({'a': True}, class_='mg-snippet__url'):
                        link = el['href']
                        # if link.find('.ua/') == -1 and link.find('.uz/') == -1:
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
                        # if link.find('.ua/') == -1 and link.find('.uz/') == -1:
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
    result = pd.DataFrame({
         'Ключевое слово': keys_for_table,
         'Ссылки': links,
         'Заголовок': titles
     })
    result = history(result)

    return result


"""ДОДЕЛАТЬ"""
# не брать индексы которые уже записаны в to_drop и тот индекс на котором находится цикл
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

    def correct_text_gettext(bs):
        pretext = bs.get_text(separator='\n', strip=True).split('\n')
        corr_text = []
        for par in pretext:
            if par.strip() and len(par.split(' ')) >= 2 and par.strip()[-1] in '!;?:,.;':
                corr_text.append(par)
        corr_text = '\n'.join(corr_text)
        return corr_text

    # def correct_text_findall(bs):
    #     pretext = []
    #     for div in bs.findAll({'div': True}):
    #         if div.find({'p': True}):
    #             for el in div.findAll({'p': True,
    #                                    'li': True,
    #                                    }):
    #                 while el.text not in pretext:
    #                     pretext.append(el.text)
    #     corr_text = []
    #     for par in pretext:
    #         if par.strip() and len(par.split(' ')) >= 2 and par.strip()[-1] in '!;?:,.;':
    #             corr_text.append(par)
    #     corr_text = '\n'.join(corr_text)
    #     return corr_text

    parsed_links = df['Ссылки'].tolist()
    texts = []
    if parsed_links:
        for link in tqdm(parsed_links):
            try:
                response = session.get(link, timeout=15)
            except IOError as http_err:
                print(f'Не удалось выгрузить данные с {link}!!! Проблемы с подключением к ресурсу: {http_err}')
                texts.append(f'Не удалось выгрузить данные!!! Проблемы с подключением к ресурсу: {http_err}')
            except Exception as err:
                print(f'Не удалось выгрузить данные с {link}!!! Ошибка: {err}')
                texts.append(f'Не удалось выгрузить данные!!! Ошибка: {err}')
            else:
                soup = BeautifulSoup(response.text, 'lxml')
                # text = correct_text_findall(soup)
                text = correct_text_gettext(soup)
                if re.search(r'[А-Яа-я]', text):
                    texts.append(text)
                else:
                    charset = chardet.detect(response.content).get('encoding')
                    response.encoding = charset
                    soup = BeautifulSoup(response.text, 'lxml')
                    # text = correct_text_findall(soup)
                    text = correct_text_gettext(soup)
                    if re.search(r'[А-Яа-я]', text):
                        texts.append(text)
                    else:
                        response.encoding = 'utf8'
                        soup = BeautifulSoup(response.text, 'lxml')
                        # text = correct_text_findall(soup)
                        text = correct_text_gettext(soup)
                        if re.search(r'[А-Яа-я]', text):
                            texts.append(text)
                        else:
                            response.encoding = 'windows-1251'
                            soup = BeautifulSoup(response.text, 'lxml')
                            # text = correct_text_findall(soup)
                            text = correct_text_gettext(soup)
                            if re.search(r'[А-Яа-я]', text):
                                texts.append(text)
                            else:
                                texts.append('В тексте отсутствуют русские символы!')

    else:
        print('Список ссылок пуст!')
        sys.exit()
    return texts


def clear_texts(df):
    for row in df.index:
        df.loc[row, 'Тексты'] = re.sub(r'\n{2,}', r'\n', df.loc[row, 'Тексты'])
    to_drop = set()
    for row in df.index:
        text = df.loc[row, 'Тексты']
        if text == 'В тексте отсутствуют русские символы!' or \
                re.search(r'Не удалось выгрузить данные!!!', text):
            to_drop.add(row)
    df.drop(to_drop, inplace=True)
    to_drop = set()
    for row in df.index:
        text = df.loc[row, 'Тексты']
        if cld2.detect(text)[2][0][0] != 'RUSSIAN':
            to_drop.add(row)
    df.drop(to_drop, inplace=True)
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
