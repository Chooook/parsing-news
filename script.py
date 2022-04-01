import os

import requests

from functions import *

if not os.path.exists(directory):
    os.makedirs(directory)

session = requests.Session()
session.headers.update(headers)

try:
    response = session.get('https://ya.ru/')
except IOError:
    print('Проблемы с интернет подключением!')
else:

    result = pd.DataFrame(columns=('Поисковик', 'Ключевое слово', 'Ссылки', 'Наименование', 'Тексты'))

    for engine in engines.__iter__():

        for filename in (
            'ml words',
            'one word',
            'two words',
            'three words'
        ):
            key_words = pd.read_csv('data/' + filename + '.csv', header=None)[0].tolist()

            for key in tqdm(key_words):

                links, titles = links_parser(session, key, engine)

                result = pd.concat(
                    [result,
                     pd.DataFrame({
                         'Поисковик': engine,
                         'Ключевое слово': key,
                         'Наименование': titles,
                         'Ссылки': links
                     })
                     ],
                    ignore_index=True
                )

    result.drop_duplicates(['Ссылки'], inplace=True, ignore_index=True)
    result.drop_duplicates(['Наименование'], inplace=True, ignore_index=True)
    # result.to_excel(directory + '/'
    #                 + 'resultsNoText.xlsx',
    #                 index=False)
    result['Тексты'] = text_parser(result, session)
    # result.to_excel(directory + '/'
    #                 + 'resultsBefore.xlsx',
    #                 index=False)
    session.close()
    result = clear_texts(result)
    result.to_excel(directory + '/'
                    + 'resultsAfter.xlsx',
                    index=False)