# -*- coding: UTF-8 -*-
from bs4 import BeautifulSoup
import urllib2
import sys
import re
import time
import datetime
# import MySQLdb
# import chardet
reload(sys)
sys.setdefaultencoding('utf8')

# date format 'YYYY-mm-dd'
start_date_str = '2016-01-01'
end_date_str = datetime.datetime.now().strftime("%Y-%m-%d")

news_data_name = 'news_data/jinrongjie_announcement_bond_' + time.strftime('%Y_%m_%d_%H_%M_%S',time.localtime())
# print news_data_name
f = open(news_data_name, 'w')

start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
time_delta = datetime.timedelta(days=1)


while start_date <= end_date:

    list_url = 'http://bond.jrj.com.cn/xwk/%s/%s_1.shtml' % (start_date.strftime("%Y%m"), start_date.strftime("%Y%m%d"))
    print list_url
    try:
        list_html = urllib2.urlopen(list_url).read()
        list_soup = BeautifulSoup(list_html, from_encoding='gb2312')
    except urllib2.HTTPError:
        print "HTTP Error:" + list_url
        start_date += time_delta
        continue

    if len(list_soup.select('ul.list > li > a')) > 0:
        assert len(list_soup.select('ul.list > li > a')) == len(list_soup.select('ul.list > li > span'))
        a_list = list_soup.select('ul.list > li > a')
        span_list = list_soup.select('ul.list > li > span')
        for i in range(len(a_list)):
            news_title = a_list[i].get_text()
            p_time = span_list[i].get_text()
            news_url = a_list[i]['href']
            try:
                news_html = urllib2.urlopen(news_url).read()
            except urllib2.HTTPError:
                print "HTTP Error:" + news_url
                continue
            news_soup = BeautifulSoup(news_html, from_encoding='gb2312')
            body = ''

            for subbody in news_soup.select('div.texttit_m1 > p'):
                body += '\n' + subbody.get_text()

            f.write('%s\n%s\n%s\n\n%s\n\n' % (news_title.encode('utf8'), p_time, news_url, body.encode('utf8')))
            f.write('%s\n\n' % (200 * '*'))

    start_date += time_delta


f.close()