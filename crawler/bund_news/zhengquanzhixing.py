# -*- coding: UTF-8 -*-
from bs4 import BeautifulSoup
import urllib2
import sys
import re
import time
# import MySQLdb
# import chardet
reload(sys)
sys.setdefaultencoding('utf8')

news_data_name = 'news_data/zhengquanzhixing_announcement_bond_' + time.strftime('%Y_%m_%d_%H_%M_%S',time.localtime())
# print news_data_name
f = open(news_data_name, 'w')


for i in range(1, 11):
    if i == 1:
        list_url = 'http://bond.stockstar.com/list/2469.shtml'
    else:
        list_url = 'http://bond.stockstar.com/list/2469_%s.shtml' % str(i)
    print list_url
    list_html = urllib2.urlopen(list_url).read()
    list_soup = BeautifulSoup(list_html, from_encoding='gb2312')
    #print len(list_soup.select('div.newslist_content > ul'))
    for item in list_soup.select('div.newslist_content > ul'):
        item_list = item.select('li')
        for i in range(len(item_list)):

            if i % 2 == 0:
                # print type(item_list[i])
                news_title = item_list[i].select('a')[0].get_text()
                news_url = item_list[i].select('a')[0]['href']

                # print news_title, news_url
            else:
                p_time = item_list[i].get_text()

                news_html = urllib2.urlopen(news_url).read()
                news_soup = BeautifulSoup(news_html, from_encoding='gb3212')

                body = ''

                # if content has table, body = table
                if len(news_soup.select('table')) > 0:
                    body = 'table'
                else:
                    for subbody in news_soup.select('div#container-article > p'):
                        body += '\n' + subbody.get_text()

                f.write('%s\n%s\n%s\n\n%s\n\n' % (news_title.encode('utf8'), p_time, news_url, body.encode('utf8')))
                f.write('%s\n\n' % (200 * '*'))


f.close()