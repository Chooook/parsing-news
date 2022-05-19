from datetime import datetime, timedelta
from time import time


class Paths:
    """Data-container with paths."""
    keys_path = 'data/key words/5keys/'
    # keys_path = 'data/key words/yake/'
    # keys_path = 'data/key words/textrank/'
    # keys_path = 'data/key words/lda/'
    keys_dir_name = keys_path.split('/')[-2]
    output_path = 'output/'
    stopwords_path = 'data/stopwords/'
    history_path = f'history/{keys_dir_name}/'
    old_history_path = 'history/old/'
    # список путей для автоматического создания папок при их отсутствии
    items = (keys_path, output_path, stopwords_path,
             history_path, old_history_path
             )


class Times:
    """Data-container with time variables."""
    today = datetime.today()
    today_str = today.strftime('%Y%m%d')
    launch_time_str = today.strftime('%Y.%m.%d %H-%M-%S')
    yesterday_str = (today - timedelta(days=1)).strftime('%Y%m%d')
    week_ago_str = (today - timedelta(days=6)).strftime('%Y%m%d')
    unix_now_str = str(int(time())) + '000'
    unix_week_str = str((int(time()) - 86400 * 6)) + '000'
    # за сколько дней будет использоваться история:
    history_delta = timedelta(days=14)
    # насколько старая история будет переноситься в old_history:
    replace_delta = timedelta(days=90)


class Engine:
    """Search-engine parent-class."""
    name = ''  # наименование поисковика
    query_start = ''  # начало url поискового запроса
    query_end_day = ''  # окончание url поискового запроса за день
    query_end_week = ''  # окончание url поискового запроса за неделю
    article_class = ''  # html-class элемента со ссылкой и заголовком

    def get_article(self, el):
        """Load link and title."""
        pass


class Ya(Engine):
    name = 'Yandex News'
    query_start = 'https://newssearch.yandex.ru/news/search?text='
    query_end_day = f'+date%3A{Times.yesterday_str}&flat=1&sortby=date' \
                    f'&filter_date={Times.unix_now_str}'
    query_end_week = f'+date%3A{Times.week_ago_str}..{Times.today_str}' \
                     '&flat=1&sortby=date&filter_date=' \
                     f'{Times.unix_week_str}%2C{Times.unix_now_str}'
    article_class = 'mg-snippet__url'

    def get_article(self, el):
        """Load link and title, using Yandex News configuration."""
        link = el['href']
        tail = link[link.find('utm') - 1:]
        link = link.replace(tail, '')
        title = el.div.span.text
        return link, title


class Bing(Engine):
    name = 'Bing News'
    query_start = 'https://www.bing.com/news/search?q='
    query_end_day = '+language%3aru&qft=interval%3d"7"&form=PTFTNR&cc=ru'
    query_end_week = '+language%3aru&qft=interval%3d"8"&form=PTFTNR&cc=ru'
    article_class = 'title'

    def get_article(self, el):
        """Load link and title, using Bing News configuration."""
        link = el['href']
        title = el.text
        return link, title
