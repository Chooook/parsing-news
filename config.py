import datetime
from time import time

# input_dir = 'data/аналитики/'
# input_dir = 'data/test/'
input_dir = 'data/key words/'

output_dir = 'output/'

links = []
titles = []
keys_for_table = []

today = datetime.date.today()
today_str = today.strftime('%Y%m%d')
yesterday = today - datetime.timedelta(days=1)
yesterday_str = yesterday.strftime('%Y%m%d')
week = today - datetime.timedelta(days=6)
week_str = week.strftime('%Y%m%d')
launch_time = datetime.datetime.today()
launch_time_str = launch_time.strftime('%Y.%m.%d %H-%M-%S')
time_now = int(time())
time_now_str = str(time_now) + '000'
time_week = time_now - 86400*6
time_week_str = str(time_week) + '000'

query_start = {
    'yandex': 'https://newssearch.yandex.ru/news/search?text="',
    'bing': 'https://www.bing.com/news/search?q="',
}
query_end = {
    # ЗА ДЕНЬ:
    # 'yandex': f'"+date%3A{yesterday_str}&flat=1&sortby=date&filter_date={time_now_str}',
    # ЗА НЕДЕЛЮ:
    'yandex': f'"+date%3A{week_str}..{today_str}&flat=1&sortby=date&filter_date={time_week_str}%2C{time_now_str}',
    # ЗА ДЕНЬ:
    # 'bing': '"+language%3aru&qft=interval%3d"7"&form=PTFTNR&cc=ru',
    # ЗА НЕДЕЛЮ:
    'bing': '"+language%3aru&qft=interval%3d"8"&form=PTFTNR&cc=ru',
}
engines = {
    'yandex': (
        query_start['yandex'],
        query_end['yandex'],
    ),
    'bing': (
        query_start['bing'],
        query_end['bing'],
    )
}
