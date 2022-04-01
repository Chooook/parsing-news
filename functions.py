import os
import re
import sys

import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tqdm import tqdm

from config import *


def connection_check(session):
    try:
        session.get('https://ya.ru/')
    except IOError:
        print('Проблемы с интернет подключением!')
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


def read_keys(files):
    keys = []
    for filename in files:
        key_words = pd.read_csv(input_dir + filename, header=None)[0].tolist()
        keys += key_words
    return keys


def links_parser(session, key, engine):
    links = []
    titles = []
    try:
        qs, qe = engines[engine]
        response = session.get(
            qs
            + key
            + qe
        )
    except IOError:
        print(f'Запрос {key} не выполнен!(Не удалось подключиться)')
    else:
        soup = BeautifulSoup(response.text, 'lxml')

        if engine == 'yandex':
            for el in soup.findAll({'a': True}, class_='mg-snippet__url'):
                link = el['href']
                if link.find('.ua/') == -1 and link.find('.uz/') == -1:
                    tail = link[link.find('utm')-1:]
                    link = link.replace(tail, '')
                    title = el.div.span.text
                    flag = False
                    if titles:
                        for elem in titles:
                            if fuzz.token_sort_ratio(elem, title) >= 60:
                                flag = True
                    else:
                        links.append(link)
                        titles.append(title)
                    if not flag:
                        links.append(link)
                        titles.append(title)

        else:
            for el in soup.findAll({'a': True}, class_='title'):
                link = el['href']
                if link.find('.ua/') == -1 and link.find('.uz/') == -1:
                    title = el.text
                    flag = False
                    if titles:
                        for elem in titles:
                            if fuzz.token_sort_ratio(elem, title) >= 60:
                                flag = True
                    else:
                        links.append(link)
                        titles.append(title)
                    if not flag:
                        links.append(link)
                        titles.append(title)

    return links, titles


def text_parser(df, session):
    links = df['Ссылки'].tolist()
    texts = []
    if links:
        for link in tqdm(links):
            try:
                response = session.get(link)
            except IOError:
                texts.append('Не удалось выгрузить данные!!!')
            else:
                response.encoding = 'utf8'
                soup = BeautifulSoup(response.text, 'lxml')
                text = []
                for div in soup.findAll({'div': True}):
                    # if div.find({'div': True}):
                    #     continue
                    if div.find({'p': True}):
                        for el in div.findAll({'p': True, 'li': True}):
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
    to_drop = []
    for row in df.index:
        text = df.loc[row, 'Тексты']
        if pd.notna(text) and text != 'Не удалось выгрузить данные!!!':
            key = df.loc[row, 'Ключевое слово']
            if not re.search(rf'\b{key.lower()}\b', text.lower()):
                to_drop.append(row)
        else:
            to_drop.append(row)
    df.drop(to_drop, inplace=True)

    return df
