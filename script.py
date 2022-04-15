import requests

from functions import *

session = requests.Session()
connection_check(session)
keys = read_keys(files_parse())

result = links_parser(session, keys)

result.drop_duplicates(['Ссылки'], inplace=True, ignore_index=True)
result['Тексты'] = text_parser(result, session)
# result.to_excel(f'{output_dir}/result_no_clear_text_{launch_time_str}.xlsx',
#                 index=False)

result = clear_texts(result)
# result.to_excel(f'{output_dir}/result_{launch_time_str}.xlsx',
#                 index=False)

session.close()

ml_words = set(pd.read_csv('data/ml/ml words.csv', header=None)[0].to_list())

# for j, row in result.iterrows():
#     row[]
#     result.loc[j, 'Тексты']

result['ML проверка'] = [check(s, ml_words) for s in result['Тексты'].values]
result.to_excel(f'{output_dir}/result_check_ml_words_{launch_time_str}.xlsx',
                index=False)
