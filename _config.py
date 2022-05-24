from dataclasses import dataclass
from datetime import datetime, timedelta
from time import time


class Paths:
    """Data-container with paths."""
    keys_path = 'data/key words/5keys/'
    # keys_path = 'data/key words/yake/'
    # keys_path = 'data/key words/textrank/'
    # keys_path = 'data/key words/lda/'
    # keys_dir_name = keys_path.split('/')[-2]
    output_path = 'output/'
    stopwords_path = 'data/stopwords/'
    # history_path = f'history/{keys_dir_name}/'
    history_path = f'history/'
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
    week_ago_str = (today - timedelta(days=6)).strftime('%Y%m%d')
    unix_now_str = str(int(time())) + '000'
    unix_week_str = str((int(time()) - 86400 * 6)) + '000'

    # за сколько дней будет использоваться история:
    history_delta = timedelta(days=14)

    # насколько старая история будет переноситься в old_history:
    replace_delta = timedelta(days=90)


@dataclass
class Engine:
    """Search-engine parent-class."""
    name: str  # наименование поисковика
    query_start: str  # начало url поискового запроса
    query_end_day: str  # окончание url поискового запроса за день
    query_end_week: str  # окончание url поискового запроса за неделю
    article_class: str  # html-class элемента со ссылкой и заголовком

    def get_article(self, el):
        """Load link and title.

        :param el: element found by soup.findAll by 'article_class'
        :type el: bs4.element.Tag
        """
        link = el['href']
        title = el.text
        return link, title


class Ya(Engine):
    name = 'Yandex News'
    query_start = 'https://newssearch.yandex.ru/news/search?text='
    query_end_day = f'+date%3A{Times.today_str}&flat=1&sortby=date' \
                    f'&filter_date={Times.unix_now_str}'
    query_end_week = f'+date%3A{Times.week_ago_str}..{Times.today_str}' \
                     '&flat=1&sortby=date&filter_date=' \
                     f'{Times.unix_week_str}%2C{Times.unix_now_str}'
    article_class = 'mg-snippet__url'

    def get_article(self, el) -> tuple[str, str]:
        """Load link and title, using Yandex News configuration.

        :param el: element found by soup.findAll by Ya.article_class
        :type el: bs4.element.Tag

        :rtype: tuple[str, str]
        :return: link and title of article
        """
        link = el['href']
        tail = link[link.find('utm') - 1:]
        link = link.replace(tail, '')
        title = el.div.span.text
        return link, title

#
# class Bing(Engine):
#     name = 'Bing News'
#     query_start = 'https://www.bing.com/news/search?q='
#     query_end_day = '+language%3aru&qft=interval%3d"7"&form=PTFTNR&cc=ru'
#     query_end_week = '+language%3aru&qft=interval%3d"8"&form=PTFTNR&cc=ru'
#     article_class = 'title'
