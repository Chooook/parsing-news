import requests
import sys
from datetime import datetime, timedelta
from time import time


class Connection:
    @staticmethod
    def session_create() -> requests.sessions.Session:
        """
        :return: 
        """
        s = requests.Session()
        Connection.check_connection(s)
        return s

    @staticmethod
    def check_connection(session: requests.sessions.Session):
        try:
            session.get('https://yandex.ru/', timeout=25)

        except IOError:
            print(f'Проблемы с интернет подключением!')
            sys.exit()

        except Exception as err:
            print(f'Ошибка:\n'
                  f'{err}')
            sys.exit()


class Dirs:
    input_dir = 'data/key words/'
    output_dir = 'output/'
    stopwords_dir = 'data/stopwords/'
    history_dir = 'history/'
    # old_history_dir = 'history/old/'
    items = (input_dir, output_dir,
             stopwords_dir, history_dir,
             # old_history_dir
             )


class Times:
    today = datetime.today()
    today_str = today.strftime('%Y%m%d')
    ytd_str = (today - timedelta(days=1)).strftime('%Y%m%d')
    week_str = (today - timedelta(days=6)).strftime('%Y%m%d')
    launch_time = datetime.today()
    launch_time_str = launch_time.strftime('%Y.%m.%d %H-%M-%S')
    unix_now_str = str(int(time())) + '000'
    unix_week_str = str((int(time()) - 86400 * 6)) + '000'
    history_timedelta = timedelta(days=14)


class Engine:
    registry = []

    def __init__(
            self,
            name: str,
            query_start: str,
            query_end_day: str,
            query_end_week: str,
            article_class: str
            ):
        self.registry.append(self)
        self.name = name
        self.query_start = query_start
        self.query_end_day = query_end_day
        self.query_end_week = query_end_week
        self.article_class = article_class


ya = (
    'Yandex News',

    'https://newssearch.yandex.ru/news/search?text=',

    f'+date%3A{Times.ytd_str}&flat=1&sortby=date'
    f'&filter_date={Times.unix_now_str}',

    f'+date%3A{Times.week_str}..{Times.today_str}'
    f'&flat=1&sortby=date&filter_date={Times.unix_week_str}'
    f'%2C{Times.unix_now_str}',

    'mg-snippet__url'
    )
bing = (
    'Bing News',

    'https://www.bing.com/news/search?q=',

    '+language%3aru&qft=interval%3d"7"&form=PTFTNR&cc=ru',

    '+language%3aru&qft=interval%3d"8"&form=PTFTNR&cc=ru',

    'title'
    )

Ya = Engine(*ya)
Bing = Engine(*bing)
