import requests

import pymorphy2

from functions import *

session = requests.Session()
session.headers.update(headers)
connection_check(session)
keys = read_keys(files_parse())

# new_keys = ['аудит', 'аудитор', 'аудиторский']
# morph = pymorphy2.MorphAnalyzer()
# keys = []
# for i in new_keys:
#     words = morph.parse(i)[0].lexeme
#     for j in words:
#         keys.append(j[0])
# keys = list(set(keys))
# print(keys)

result = pd.DataFrame(columns=(
    'Ключевое слово',
    'Ссылки',
    'Заголовок',
    'Тексты'
))
morph = pymorphy2.MorphAnalyzer()

for engine in engines.__iter__():

    # print(f'Выгрузка ссылок с помощью поисковика {engine}:\n')

    # for key in tqdm(keys):
    links, titles, keys_for_table = links_parser(session, keys, engine, morph)

    result = pd.concat(
        [result,
         pd.DataFrame({
             # 'Поисковик': engine,
             'Ключевое слово': keys_for_table,
             'Заголовок': titles,
             'Ссылки': links
         })
         ],
        ignore_index=True
    )

result.drop_duplicates(['Ссылки'], inplace=True, ignore_index=True)
# result = drop_by_titles(result)
# result.drop_duplicates(['Заголовок с поисковика'], inplace=True, ignore_index=True)
# result.to_excel(directory + '/'
#                 + 'resultsNoText.xlsx',
#                 index=False)
result['Тексты'] = text_parser(result, session)
# result.to_excel(output_dir + '/'
#                 + 'results_clear_text_findAll.xlsx',
#                 index=False)
result = clear_texts(result)
result.to_excel(output_dir + '/'
                + 'results_.xlsx',
                index=False)

session.close()
