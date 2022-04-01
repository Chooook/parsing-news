import requests

from functions import *


files = files_parse()
keys = read_keys(files)

session = requests.Session()
session.headers.update(headers)

try:
    response = session.get('https://ya.ru/')
except IOError:
    print('Проблемы с интернет подключением!')
else:

    result = pd.DataFrame(columns=('Поисковик', 'Ключевое слово', 'Ссылки', 'Заголовок', 'Тексты'))

    for engine in engines.__iter__():

        for key in tqdm(keys):

            links, titles = links_parser(session, key, engine)

            result = pd.concat(
                [result,
                 pd.DataFrame({
                     'Поисковик': engine,
                     'Ключевое слово': key,
                     'Заголовок': titles,
                     'Ссылки': links
                 })
                 ],
                ignore_index=True
            )

    result.drop_duplicates(['Ссылки'], inplace=True, ignore_index=True)
    result.drop_duplicates(['Заголовок'], inplace=True, ignore_index=True)
    # result.to_excel(directory + '/'
    #                 + 'resultsNoText.xlsx',
    #                 index=False)
    result['Тексты'] = text_parser(result, session)
    # result.to_excel(directory + '/'
    #                 + 'resultsBefore.xlsx',
    #                 index=False)
    session.close()
    result = clear_texts(result)
    result.to_excel(output_dir + '/'
                    + 'resultsAfter.xlsx',
                    index=False)
