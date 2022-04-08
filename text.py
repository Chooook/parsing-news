import re
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import pandas as pd
from config import *

session = requests.Session()
session.headers.update(headers)
df = pd.read_excel('output/resultsAfter.xlsx')
texts = []
titles = []
if not df.empty:
    for row in tqdm(df.index):
        link = df.loc[row, 'Ссылки']
        try:
            response = session.get(link)
        except IOError:
            texts.append('Не удалось выгрузить данные!!!')
            titles.append('Не удалось выгрузить данные!!!')
        else:
            response.encoding = 'utf8'
            soup = BeautifulSoup(response.text, 'lxml')
            title = soup.findAll({'h1': True})
            temp_title = []
            for el in title:
                if el.text:
                    title_cor = re.sub(r'\xa0', ' ', el.text)
                    # title_cor = re.sub(r'[\r\t\n]', '', title_cor)
                    title_cor = re.sub(r'\r', '', title_cor)
                    title_cor = re.sub(r'\t', '', title_cor)
                    title_cor = re.sub(r'\n', '', title_cor)
                    temp_title.append(title_cor.strip())
            if len(temp_title) == 1:
                titles.append(temp_title[0])
            else:
                titles.append(df.loc[row, 'Заголовок с поисковика'])

            text = soup.get_text(separator='\n', strip=True).split('\n')
            search_title = df.loc[row, 'Заголовок с поисковика'].strip
            correct_text = []
            flag = False
            if len(temp_title) == 1:
                for par in text:
                    if not flag:
                        while re.search(rf'{temp_title}', par) and not flag:
                            flag = True
                            correct_text.append(par)
                        continue
                    else:
                        correct_text.append(par)
            if not correct_text and temp_title != search_title:
                for par in text:
                    if not flag:
                        while re.search(rf'{search_title}', par) and not flag:
                            flag = True
                            correct_text.append(par)
                        continue
                    else:
                        correct_text.append(par)
            if not correct_text:
                texts.append('Не удалось выгрузить текст!!!')
            else:
                correct_text = '\n'.join(correct_text)
                texts.append(correct_text)
    df['Тексты новые'] = texts
    df['Заголовок со страницы'] = titles
else:
    print('Список ссылок пуст!')

session.close()

# for row in df.index:
#     df.loc[row, 'Тексты'] = re.sub(r'\n{2,}', r'\n', df.loc[row, 'Тексты'])
# to_drop = []
# for row in df.index:
#     text = df.loc[row, 'Тексты']
#     if text and pd.notna(text) and text != 'Не удалось выгрузить данные!!!':
#         key = df.loc[row, 'Ключевое слово']
#         if key.upper() == key:
#             if not re.search(rf'\b{key}\b', text):
#                 to_drop.append(row)
#         else:
#             if not re.search(rf'\b{key.lower()}\b', text.lower()):
#                 to_drop.append(row)
#     else:
#         to_drop.append(row)
# df.drop(to_drop, inplace=True)


def clear_texts(df):
    for row in df.index:
        df.loc[row, 'Тексты новые'] = re.sub(r'\n{2,}', r'\n', df.loc[row, 'Тексты новые'])
    to_drop = []
    for row in df.index:
        text = df.loc[row, 'Тексты новые']
        if pd.notna(text) and text != 'Не удалось выгрузить данные!!!' and text != 'Не удалось выгрузить текст!!!':
            key = df.loc[row, 'Ключевое слово']
            if not re.search(rf'\b{key.lower()}\b', text.lower()):
                to_drop.append(row)
        else:
            to_drop.append(row)
    df.drop(to_drop, inplace=True)

    return df


df = clear_texts(df)

df.to_excel(output_dir + '/'
            + 'newafter.xlsx',
            index=False)
