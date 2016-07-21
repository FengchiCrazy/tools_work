#coding:utf-8
import sys, os
import MySQLdb
import logging
import sys, os
import jieba
import jieba.analyse 
import datetime
import MySQLdb
import pdb
reload(sys)
sys.setdefaultencoding('utf8')

jieba.load_userdict('shortNamesForLoading.dic')

def getStopList():
    stopwords = ['\r','\n','\t',' ','\r\n',u'有限公司',u'公司',u'有限责任']
    stopfile = open('STOPWORDS')
    res = set([x.strip() for x in stopfile.readlines()] + stopwords)
    return res

def tokenFilter(token):
    token = token.lower()
    return token

def cutFullname(word, stoplist=set()):
    tokens = []
    gene = jieba.cut(word, cut_all=False)
    for token in gene:
        if token not in stoplist:
            tokens.append(tokenFilter(token))
    return ' '.join([ x.decode('latin1') for x in tokens] )
   

if __name__=='__main__':
    pass
