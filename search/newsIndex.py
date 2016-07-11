# coding:utf-8

from __future__ import unicode_literals
import sys, os
import jieba
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import *
from whoosh import qparser
from whoosh.qparser import QueryParser
from whoosh.query import *
from jieba.analyse import ChineseAnalyzer
from whoosh.qparser import plugins
from whoosh.sorting import *
import datetime
import MySQLdb
import pdb
import logging

STOPWORDS = ['\r','\n','\t',' ','\r\n']

class publicOpinionBase(object):
    '''
    schema:
        mysqlID  : 文档在mysql中的唯一标识，前面加一个字母标识符，'N'表示新闻类，'W'表示微信类
        platform : 标识文档来源，0表示新闻类，1表示微信类
        title    : 文档标题
        url      : 文档地址
        content  : 文档内容
        notice_time : 文档发布时间
        source   : 文档来源网站
        feed_type : 文档类型：新闻、公告等
        sentiment: 正负情感分数
        location : 地域
        main_body: 主体
        product_info : 债券信息

    '''
    def __init__(self):    
        self.__connect()
        dirpath                 = os.path.split(os.path.realpath(__file__))[0]
        self.dirpath            = os.path.dirname(dirpath)
        self.searchEnginePath   = "search_engine"
        self.indexName          = "publicOpinion" 
        self.analyzer           = self.__getAnalyzer()
        self.updateMorning      = '09:00'
        self.updateNight        = '17:30'
        self.DATE_FORMAT        = "%Y-%m-%d"
        self.TIME_FORMAT        = "%Y-%m-%d %H:%M"
        self.schema             = Schema(mysqlID=ID(stored=True),                                           \
                                         platform=NUMERIC(int, 32, stored=True),                            \
                                         title=TEXT(stored=True, analyzer=self.analyzer, field_boost=1.0),  \
                                         url=ID(stored=True),                                               \
                                         content=TEXT(stored=True, analyzer=self.analyzer),                 \
                                         notice_time=DATETIME(stored=True),                \
                                         source=TEXT(stored=True, analyzer=self.analyzer),                  \
                                         feed_type=NUMERIC(int, 32, stored=True),                           \
                                         sentiment=NUMERIC(float, 64, stored=True, field_boost=0.5),        \
                                         location=TEXT(stored=True, analyzer=self.analyzer, field_boost=0.2),               \
                                         main_body=TEXT(stored=True, analyzer=self.analyzer, field_boost=1.2),              \
                                         product_info=TEXT(stored=True, analyzer=self.analyzer, field_boost=1.1)            \
                                         )
        self.__getIndex()       
        #self.logger             = self._getLogger('SEARCH_ENGINE.log')

    def _getLogger(self, filename):
        level = logging.DEBUG
        file_path = self.dirpath + os.sep + 'data' + os.sep + filename
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

        logger = logging.getLogger()
        logger.setLevel(level)
    
        fh = logging.FileHandler(file_path, mode = 'a')
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        return logger
        
    def __getAnalyzer(self): 
        stopwords = []
        stopwordsFile = open(self.dirpath+os.sep+'data/STOPWORDS').readlines()
        siz = len(stopwordsFile)
        for ix in range(siz-1):
            word = stopwordsFile[ix].strip().decode('utf-8')
            stopwords.append(word)
        stopwords.append(stopwordsFile[-1])
        for key in STOPWORDS:
            stopwords.append(key)
        analyzer = ChineseAnalyzer(stoplist = stopwords)
        return analyzer

    def __getIndex(self):
        path = self.dirpath + os.sep + self.searchEnginePath
        if exists_in(path, self.indexName):
            self.index = open_dir(path, self.indexName)
        else:
            self.index = create_in(path, self.schema, self.indexName)
    
    def __connect(self):
        conn = MySQLdb.connect(
            host   = '127.0.0.1',
            user   = 'root',
            passwd = '',
            db     = 'test',
            charset= 'utf8'
        )
        cursor = conn.cursor()
        conn.set_character_set('utf8')
        cursor.execute('SET NAMES utf8;')
        cursor.execute('SET CHARACTER SET utf8;')
        cursor.execute('SET character_set_connection=utf8;')
        self.conn = conn
        self.cursor = cursor
        self.tableNews  = "news_crawl_temp"
        self.tableWeixin = "news_weixin3"

    def addDocumentsFromMysql(self, start='2016-06-30', end=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')):
        
        self.logger = self._getLogger('SEARCH_ENGINE.log')
        writer = self.index.writer()
        reader = self.index.reader()
        #step1: source from news 
        id_terms = set(reader.field_terms('mysqlID'))
        sql = "select   news_id,     \
                        title,       \
                        url,         \
                        body,        \
                        notice_time, \
                        source,      \
                        type,        \
                        neg_sentiment,     \
                        ner_location,      \
                        ner_org_name,      \
                        ner_company_name,  \
                        ner_product_name   \
                        from %s\
                        where notice_time>='%s'  \
                        and notice_time<='%s'    \
                        order by notice_time desc" %(self.tableNews, start, end)
        self.cursor.execute(sql)
        documents = self.cursor.fetchall()
        for doc in documents:
            try:
                mysqlID = 'N'+str(doc[0])
                if mysqlID in id_terms:
                    continue
                else:
                    id_terms.add(mysqlID)
                title   = unicode(doc[1])
                path    = unicode(doc[2])
                content = unicode(doc[3])
                source  = unicode(doc[5])
                feed_type = doc[6]
                sentiment = doc[7] if doc[7] is not None else 0
                location  = unicode(doc[8])
                if doc[9] is None and doc[10] is None:
                    main_body = None
                else:
                    orgname = unicode(doc[9]) if doc[9] is not None else ''
                    comname = unicode(doc[10]) if doc[10] is not None else ''
                    main_body = orgname + '+:' + comname
                product_info = unicode(doc[11]) if doc[11] is not None else None
                writer.add_document(
                    mysqlID = mysqlID,
                    platform= 1,
                    title   = title,
                    url     = path,
                    content = content,
                    notice_time = doc[4],
                    source      = source,
                    feed_type   = feed_type,
                    sentiment   = sentiment,
                    location    = location,
                    main_body   = main_body,
                    product_info= product_info
                    )
                self.logger.info("Insert record : %s"%mysqlID)
            except Exception,e:
                self.logger.error("ERROR:%s"%(str(e)))
                continue

        #step2:source from weixin
        sql = "select   id,     \
                        title,       \
                        url,         \
                        body,        \
                        notice_time,   \
                        source          \
                        from %s          \
                        where notice_time>='%s'  \
                        and notice_time<='%s'    \
                        order by notice_time desc" %(self.tableWeixin, start, end)
        self.cursor.execute(sql)
        documents = self.cursor.fetchall()
        for doc in documents:
            try:
                mysqlID = 'W'+str(doc[0])
                if mysqlID in id_terms:
                    continue
                else:
                    id_terms.add(mysqlID)
                title   = unicode(doc[1])
                path    = unicode(doc[2])
                content = unicode(doc[3])
                source  = unicode(doc[5])
                writer.add_document(
                    mysqlID = mysqlID,
                    platform= 2,
                    title   = title,
                    url     = path,
                    content = content,
                    notice_time = doc[4],
                    source      = source,
                    sentiment   = 0
                    )
                self.logger.info("Insert record : %s"%mysqlID)

            except Exception,e:
                self.logger.error("ERROR:%s"%(str(e)))
                continue
        writer.commit()
            
    def parse_search(self, search_string):
        """
        实现了以下功能：
        逻辑表示： & 表示且， | 表示或， &! 表示不包含随后关键词的结果， &~ 表示后面一项可包含可不包含，若包含则结果排名靠前
        前缀匹配： 输入适当词的前缀之后， 通过*符号达到匹配的目的。 如"水*"可以匹配到"水果"
        模糊匹配： 查询的关键词后跟~表示进行模糊匹配。默认的模糊距离为1，即现有词与模糊匹配的词有一个字不同。
                  如'水果~'可以匹配到'水平'和'苹果'
                  可以在~后加数字设置模糊匹配的最大距离。但是当超过2时这种模糊匹配的速度就会很慢，且通常与你的结果相距甚远。
        域匹配： 默认是在内容中查找，可以通过关键词'title:'查询题目
        时间范围：通过关键词'date:'设置查询日期范围，查询日期的格式必须为YYYYmmdd
                如查找2016年6月1日以来的所有结果，通过设置'date:>=20160601'， 中间不能出现空格。
        权重分配： 通过'^'后加数字的格式，人为设置不同关键词之间的权重，使得权重大的结果在出现在搜索结果前列。
        查询分组：通过'()'使查询结果进行分组。

        :param search_string: 查询字符串
        :param schema: ：class:'whoosh.fields.Schema'
        :return: :class:`whoosh.query.Query`
        """
        schema = self.schema
        my_plugin = [
            plugins.WhitespacePlugin(), 
            plugins.SingleQuotePlugin(), #''
            plugins.FieldsPlugin(),      #:
            plugins.WildcardPlugin(),    #*
            #plugins.PhrasePlugin(),      #""
            #plugins.GroupPlugin(),      #()
            plugins.BoostPlugin(),       #^
            #plugins.EveryPlugin(),
            plugins.OperatorsPlugin(And="&", Or="\|", AndNot="&[!！]", AndMaybe="&~", Not=None), #& | ! ~
            plugins.RangePlugin(),       #
            plugins.FuzzyTermPlugin(),
            plugins.GtLtPlugin()
        ]
        parser = qparser.QueryParser("content", schema, plugins=my_plugin)

        return parser.parse(search_string)

    def _getDatetimeRange(self, date):
        morning_checktime_str = date.strftime(self.DATE_FORMAT) + ' ' + self.updateMorning
        morning_checktime     = datetime.datetime.strptime(morning_checktime_str, self.TIME_FORMAT)
        if date > morning_checktime:
            dateStart = morning_checktime - datetime.timedelta(seconds = 60 * 60)
            dateEnd   = date
            am = False
        else:
            delta_day = datetime.timedelta(days = 1)
            yesterday_str = (date - delta_day).strftime(self.DATE_FORMAT)
            yesterday_start_str = yesterday_str + ' ' + self.updateNight
            yesterday_start = datetime.datetime.strptime(yesterday_start_str, self.TIME_FORMAT)
            dateStart = yesterday_start - datetime.timedelta(seconds = 60 * 60)
            dateEnd   = date
            am = True
        return dateStart, dateEnd, am

    def search(self):
        searcher = self.index.searcher()
        kw1="notice_time:>='20160701000000' notice_time:<='20160702100000' & platform:1"
        #pdb.sezhutit_trace()
        #keyword = "风险&控股 & notice_time:>='2016-06-20' notice_time:<='2016-06-27' &~title:市场 &~title:银监会"
        #kw2 = "title:上市 | title:招标 | title:缴款 | title:到期"
        zhuti = "08北辰债~"
        diyu  = "中国|北京|重庆"
        zhaiquan = "易方达01~|博时09~"
        kw2 = "main_body:春和集团|天威称集团"
        qu = self.parse_search(kw1)
        qu2 =self.parse_search(kw2)

        #f1=FieldFacet("notice_time",reverse=True)
        f1=ScoreFacet()
        f2=FieldFacet("sentiment",reverse=True)
        f3=FieldFacet("notice_time",reverse=True)
        facet = MultiFacet([f1,f2,f3])
        results = searcher.search(qu&qu2, limit=100, sortedby=facet)
        for hit in results:
            title = hit.get('title', None).strip().decode('latin-1').encode('utf8','ignore')
            url   = hit.get('url', None).strip().decode('latin-1').encode('utf8','ignore')
            time  = hit.get('notice_time', None)
            print "%s\t%s\t%s\t%s\t%s" %(title, url, time, hit.score, hit.get('sentiment',None))

    def feedsPush(self, result):
        pass
     

def runDaily():
    model = publicOpinionBase()
    dateEnd = datetime.datetime.now()
    dateStart = dateEnd - datetime.timedelta(days=1)
    model.addDocumentsFromMysql(dateStart.strftime('%Y-%m-%d %H:%M:%S'), dateEnd.strftime('%Y-%m-%d %H:%M:%S'))

def runHistory():
    model = publicOpinionBase()
    dateEnd = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dateStart = "2016-01-01"
    model.addDocumentsFromMysql(dateStart, dateEnd)
    #model.search()
 
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    runDaily()
    #runHistory()
