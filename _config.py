from datetime import datetime as dt, timedelta

import pandas as pd


class Paths:
    """Data-container with paths."""
    keys_path = f'data/keywords/{input("Директория с ключевыми словами: ")}/'
    keys_dir_name = keys_path.split('/')[-2]
    output_path = 'output/'
    stopwords_path = 'data/stopwords/'
    history_path = f'history/{keys_dir_name}/'
    # history_path = f'history/'
    old_history_path = 'history/old/'

    # список путей для автоматического создания папок при их отсутствии
    items = (keys_path, output_path, stopwords_path,
             history_path, old_history_path)


class Times:
    """Data-container with time variables."""

    @staticmethod
    def __get_data(desc: str = 'вашу') -> dt.date:
        date = None

        while not date:
            try:
                date = dt.strptime(
                    input(f'Введите {desc} дату в (формате ddmmYYYY): '),
                    '%d%m%Y'
                )

            except ValueError:
                print('Неверный формат даты!')

        return date

    @staticmethod
    def __get_list_of_dates(
            first_date: dt,
            second_date: dt
    ) -> list[dt]:

        dates_list = []

        if first_date == second_date:
            dates_list.append(first_date)
            return dates_list

        if first_date > second_date:
            first_date, second_date = second_date, first_date

        dates_list = pd.date_range(first_date, second_date).to_list()

        return dates_list

    first_date = __get_data('первую')
    second_date = __get_data('вторую')
    dates_list = __get_list_of_dates(first_date, second_date)
    today = dt.today()
    today_str = today.strftime('%Y%m%d')
    launch_time_str = today.strftime('%Y.%m.%d %H-%M-%S')

    # за сколько дней будет использоваться история:
    history_delta = timedelta(days=14)

    # насколько старая история будет переноситься в old_history:
    replace_delta = timedelta(days=90)


class Ya:
    """Search-engine class."""
    # наименование поисковика
    name = 'Yandex News'
    # начало url поискового запроса
    query_start = 'https://newssearch.yandex.ru/news/search?text='
    # html-class элемента со ссылкой и заголовком
    article_class = 'mg-snippet__url'

    @staticmethod
    def query_end(date: dt):
        qend = (
            f'+date%3A{date.strftime("%Y%m%d")}..{date.strftime("%Y%m%d")}'
            '&flat=1&sortby=date&filter_date='
            f'{int(date.timestamp() * 1000)}%2C{int(date.timestamp() * 1000)}'
        )
        return qend

    @staticmethod
    def get_article(el) -> tuple[str, str]:
        """Load link and title, using Yandex News configuration."""
        link = el['href']
        tail = link[link.find('utm') - 1:]
        link = link.replace(tail, '')
        title = el.div.span.text
        return link, title
