# -*- coding:utf-8 -*-

from bs4 import BeautifulSoup
import urllib2
import sys
import re
import time

reload(sys)
sys.setdefaultencoding('utf8')

start_url = "http://www.cs.com.cn/gppd/zqxw/"
news_data_name = 'news_data/zhongguozhengquan_announcement_bond_' + time.strftime('%Y_%m_%d_%H_%M_%S',time.localtime())
print news_data_name
f = open(news_data_name, 'w')

# first page
home_html = urllib2.urlopen(start_url).read()
soup = BeautifulSoup(home_html, from_encoding='gb3212')
print soup.select('div.column-box > ul > li > a')
print len(soup.select('div.column-box > ul > li'))

for item in soup.select('div.column-box > ul > li > span'):
    #p_time = item.get_text()
    news_url = start_url + item.nextSibling['href'][2:]
    news_title = item.nextSibling.get_text()
    # print news_url
    # print news_title
    new_html = urllib2.urlopen(news_url).read()
    news_soup = BeautifulSoup(new_html)
    body = ''
    for subbody in news_soup.select('div.z_content > p'):
        body += '\n' + subbody.get_text()
    p_time = news_soup.select('div.column-sub > span.ctime01')[0].get_text().split('|')[0]
    #print body
    #print p_time

    f.write('%s\n%s\n%s\n\n%s\n\n' % (news_title.encode('utf8'), p_time, news_url, body.encode('utf8')))
    f.write('%s\n\n' % (200 * '*'))


# next 9 pages
for i in range(1, 10):
    page_url = start_url + 'index_' + str(i) + '.html'
    page_html = urllib2.urlopen(page_url).read()
    page_soup = BeautifulSoup(page_html, from_encoding='gb2312')
    print page_url

    for item in page_soup.select('div.column-box > ul > li > span'):
        p_time = item.get_text()
        news_url = start_url + item.nextSibling['href'][2:]
        news_title = item.nextSibling.get_text()
        # print news_url
        # print news_title
        news_html = urllib2.urlopen(news_url).read()
        news_soup = BeautifulSoup(news_html)
        body = ''
        for subbody in news_soup.select('div.z_content > p'):
            body += '\n' + subbody.get_text()
        try:
            p_time = news_soup.select('div.column-sub > span.ctime01')[0].get_text().split('|')[0]
        except:
            print news_title, news_url, news_html
            print news_soup.select('div.column-sub > span.ctime01')
        # print body
        # print p_time

        f.write('%s\n%s\n%s\n\n%s\n\n' % (news_title.encode('utf8'), p_time, news_url, body.encode('utf8')))
        f.write('%s\n\n' % (200 * '*'))

f.close()








