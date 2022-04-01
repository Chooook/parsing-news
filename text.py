import re
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import pandas as pd
from config import *

session = requests.Session()
session.headers.update(headers)
df = pd.read_excel('output/resultsNoText.xlsx')
texts = []
if not df.empty:
    for row in tqdm(df.index):
        link = df.loc[row, 'Ссылки']
        try:
            response = session.get(link)
        except IOError:
            texts.append('Не удалось выгрузить данные!!!')
        else:
            response.encoding = 'utf8'
            soup = BeautifulSoup(response.text, 'lxml')
            text = soup.get_text(separator='\n', strip=True).split('\n')
            title = df.loc[row, 'Наименование']
            correct_text = []
            flag = False
            for par in text:
                # if not flag:
                #     while par != title:
                #         flag = False
                #         continue
                #     flag = True
                # else:
                if par.strip():
                    correct_text.append(par)
            correct_text = '\n'.join(correct_text)
            texts.append(correct_text)
    df['Тексты'] = texts
else:
    print('Список ссылок пуст!')

session.close()

for row in df.index:
    df.loc[row, 'Тексты'] = re.sub(r'\n{2,}', r'\n', df.loc[row, 'Тексты'])
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

df.to_excel(output_dir + '/'
            + 'newDF1.xlsx',
            index=False)
