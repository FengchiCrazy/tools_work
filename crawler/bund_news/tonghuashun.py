# -*- coding:utf-8 -*-

from bs4 import BeautifulSoup
import urllib2
import sys
import re
import time

reload(sys)
sys.setdefaultencoding('utf8')

r_host      = '10.4.16.133'
r_user      = 'root'
r_passwd  = 'renren123'
r_name      = 'test1'

start_url = "http://bond.10jqka.com.cn/zqzx_list/index_130.shtml"
news_data_name = 'news_data/tonghuashun_announcement_bond_' + time.strftime('%Y_%m_%d_%H_%M_%S',time.localtime())
# print news_data_name
f = open(news_data_name, 'w')
list_html = urllib2.urlopen(start_url).read()



date_pattern = re.compile("\d{8}")



for i in range(1, 345):
    if i == 1:
        list_url = 'http://bond.10jqka.com.cn/zqzx_list/index.shtml'
    else:
        list_url = 'http://bond.10jqka.com.cn/zqzx_list/index_%s.shtml' % str(i)
    print list_url
    list_html = urllib2.urlopen(list_url).read()
    list_soup = BeautifulSoup(list_html, from_encoding='GBK')
    for item in list_soup.select('div.list-con > ul > li > span.arc-title'):
        news_title = item.select('a')[0]['title']
        news_url = item.select('a')[0]['href']

        date = date_pattern.search(news_url)    .group()
        # print date
        if date[:2] == '20':
            p_time = date[:4] + u'年' + item.select('span')[0].get_text()

            # print p_time
        else:
            print "Date error in url " + news_url
            continue
            #exit()

        try:
            news_html = urllib2.urlopen(news_url).read()
        except:
            print "Error url " + news_title + ' ' + news_url
            continue

        news_soup = BeautifulSoup(news_html, from_encoding='GBK')
        body = ''

        if len(news_soup.select('div.atc-content')) > 0:
            for subbody in news_soup.select('div.atc-content > p'):
                body += '\n' + subbody.get_text()
        elif len(news_soup.select('div.article-con > p')) > 0:
            for subbody in news_soup.select('div.article-con > p'):
                body += '\n' + subbody.get_text()
        elif len(news_soup.select('div.art_main > p')) > 0:
            for subbody in news_soup.select('div.art_main > p'):
                body += '\n' + subbody.get_text()
        else:
            print "Special page " + news_title + ' ' + news_url
            continue

        f.write('%s\n%s\n%s\n\n%s\n\n' % (news_title.encode('utf8'), p_time, news_url, body.encode('utf8')))
        f.write('%s\n\n' % (200 * '*'))

f.close()


