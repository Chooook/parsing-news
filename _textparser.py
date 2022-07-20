import re
import sys

import chardet
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from _utils import Utils


class TextParser:
    # определение кодировки в редких случаях работает с ошибками
    # необходимо решить проблему (проверка не дает правильную оценку)
    # __charsets = ['utf-8', 'windows-1251', 'iso-8859-5']
    __session = None
    __df = pd.DataFrame()

    @classmethod
    def parser(cls, df: pd.DataFrame):
        cls.__session = Utils.create_session()
        cls.__df = df
        if cls.__df['Ссылка'].empty:
            print('Список ссылок пуст!')
            sys.exit()
        texts = []
        for key, link in tqdm(list(zip(
                cls.__df['Ключевое слово'].values,
                cls.__df['Ссылка'].values
                )),
                desc='Выгрузка текстов'
                ):
            texts.append(TextParser.__get_text(link))
        cls.__session.close()

        cls.__df['Текст'] = texts

        for row, text, key in zip(cls.__df.index,
                                  cls.__df['Текст'].values,
                                  cls.__df['Ключевое слово'].values):
            cls.__df.loc[row, 'Текст'] = TextParser.__correct_text(text, key)

        return cls.__df

    @classmethod
    def __get_text(cls, link):
        """Return text for link if possible, or str with error message.

        :param link: article link
        :type link: str

        :rtype: str
        :return: text from article webpage or error str
        """
        try:
            response = cls.__session.get(link, timeout=15)
        except IOError as http_err:
            print(f'Не удалось выгрузить данные с {link}!!!')
            Utils.check_connection(cls.__session)
            return str('Не удалось выгрузить данные!!!'
                       f' Проблемы с подключением к ресурсу: {http_err}.')
        except Exception as err:
            print(f'Не удалось выгрузить данные с {link}!!!')
            Utils.check_connection(cls.__session)
            return f'Не удалось выгрузить данные!!! Ошибка: {err}.'
        else:
            cdet_charset = [chardet.detect(response.content).get('encoding')]
            for charset in [response.encoding] + cdet_charset:
                response.encoding = charset
                soup = BeautifulSoup(response.text, 'lxml')
                text = soup.get_text(separator='\n', strip=True)
                if re.search(r'[А-Яа-я]', text):
                    return text
            return 'Не удалось выгрузить русский текст!'

    @staticmethod
    def __correct_text(text, key):
        """Return more readable article text.

        :param text: raw article text
        :type text:str
        :param key: keyword, used to find article
        :type key: str

        :rtype: str
        :return: corrected article text
        """
        pretext = text.split('\n')
        corr_text = []
        key_clear = re.sub(r'"', '', key)

        for par in pretext:
            if (((par.strip() and len(par.split(' ')) >= 2 and
                    par.strip()[-1] in '!;?:,.') or
                    re.search(rf'\b{key_clear}\b', par.lower())) and
                    par not in corr_text):
                corr_text.append(par)
        corr_text = '\n'.join(corr_text)
        corr_text = re.sub(r'\n{2,}', r'\n', corr_text)

        return corr_text
