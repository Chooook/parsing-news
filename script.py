import requests

from functions import *

session = requests.Session()
session.headers.update(headers)
connection_check(session)
keys = read_keys(files_parse())

result = pd.DataFrame(columns=('Поисковик',
                               'Ключевое слово',
                               'Ссылки',
                               'Заголовок',
                               'Тексты'))

for engine in engines.__iter__():

    print(f'Выгрузка ссылок с помощью поисковика {engine}:\n')

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
# result.drop_duplicates(['Заголовок с поисковика'], inplace=True, ignore_index=True)
# result.to_excel(directory + '/'
#                 + 'resultsNoText.xlsx',
#                 index=False)
result['Тексты'] = text_parser(result, session)
# result.to_excel(output_dir + '/'
#                 + 'resultsBefore.xlsx',
#                 index=False)
session.close()
result = clear_texts(result)
result.to_excel(output_dir + '/'
                + 'resultsAfter.xlsx',
                index=False)
