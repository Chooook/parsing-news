import requests

from functions import *

session = requests.Session()
connection_check(session)
keys = read_keys(files_parse())

result = links_parser(session, keys)

result.drop_duplicates(['Ссылки'], inplace=True, ignore_index=True)
result['Тексты'] = text_parser(result, session)
# result.to_excel(f'{output_dir}/result_no_clear_text_{launch_time}.xlsx',
#                 index=False)

result = clear_texts(result)
result.to_excel(f'{output_dir}/result_{launch_time}.xlsx',
                index=False)

session.close()
