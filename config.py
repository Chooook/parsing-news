import datetime
from time import time

input_dir = 'data/'
output_dir = 'output/'

yesterday = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y%m%d')
now = str(int(time()))
"""Возможно, неправильно считается unix-time(now), т.к. не уверен, какое должно использоваться в фильтре по яндексу"""

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:98.0) Gecko/20100101 Firefox/98.0"
}
query_start = {
    'yandex': 'https://newssearch.yandex.ru/news/search?text="',
    'bing': 'https://www.bing.com/news/search?q="',
    # 'google': 'https://news.google.com/search?q="',
}
query_end = {
    'yandex': '"+date%3A' + yesterday + '&flat=1' + '&filter_date=' + now + '000',
    'bing': '"&qft=interval%3d"7"&form=PTFTNR&cc=ru',
    # 'google': '" when%3A1d&hl=ru&gl=RU&ceid=RU%3Aru',
}
engines = {
    'yandex': (
        query_start['yandex'],
        query_end['yandex'],
    ),
    'bing': (
        query_start['bing'],
        query_end['bing'],
    ),
    # 'google': (
    #     query_start['google'],
    #     query_end['google'],
    # ),
}
