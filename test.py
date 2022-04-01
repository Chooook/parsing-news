# import feedparser
#
#
# news = feedparser.parse('https://luki.ru/news/rss.xml')
# entry = news.entries[1]
# print(entry.keys())
# print(entry['summary_detail'])
from config import *
import requests
from pprint import pprint
from bs4 import BeautifulSoup


session = requests.Session()
session.headers.update(headers)
link = 'https://susanin.news/udmurtia/other/20220329-291218/'
response = session.get(link)
response.encoding = 'utf8'
htmlString = response.text

soup = BeautifulSoup(htmlString, 'lxml')
pprint(soup)
# r = 'ÐÑÐ¾Ð²ÐµÑÐºÐ° Ð°Ð²ÑÐ¾Ð¼Ð°ÑÐ¸ÑÐµÑÐºÐ°Ñ. ÐÑÐºÐ¾ÑÐµ Ð²Ñ Ð±ÑÐ´ÐµÑÐµ Ð¿ÐµÑÐµÐ½Ð°Ð¿ÑÐ°Ð²Ð»ÐµÐ½Ñ Ð½Ð° Ð·Ð°Ð¿ÑÐ°ÑÐ¸Ð²Ð°ÐµÐ¼ÑÐ¹ ÑÐµÑÑÑÑ.'
# print(r.decode())