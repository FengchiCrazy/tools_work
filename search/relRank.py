# coding:utf-8

#from __future__ import unicode_literals
from newsIndex import publicOpinionBase
import pdb
import whoosh.scoring as scoring
from whoosh.sorting import *
import datetime
from whoosh.query import *
import pickle
import os

class correlationRank(publicOpinionBase):
    
    def __init__(self):
        super(correlationRank, self).__init__()
        #self.user_result = {}
        self.user_words_cut = {}
        self.MAX_KEYWORDS = 100
        self.shortNamesFilterSet = self.getFilterSetFromFile(self.dirpath + os.sep + 'data' + os.sep + 'ShortNameNeedFilter.dic')
        self.userTable = "news_user_keyword"
        self.bondTable = "bond_info"
        self.companyTable = "pub_institutioninfo"
        self.correlationTable = "news_correlation_temp"
        self.userEmailTable = "news_user_email"

    def getFilterSetFromFile(self, filepath):
        filter_file = open(filepath, 'r')
        ret = set()
        for line_ in filter_file:
            word = line_.strip().decode('utf8')
            ret.add(word)
        return ret
        
    def getUserName(self,uid):
        username_sql = "select user_name from news_subscibe where uid = %s " % uid
        self.cursor.execute(username_sql)
        username_res = self.cursor.fetchall()
        try:
            un_ret = username_res[0][0]
        except:
            un_ret = None
        return un_ret

    def getFullNamesOfBondByUserID(self, uid):
        try:
            sql = "SELECT DISTINCT B.fullname FROM %s A, %s B WHERE A.uid = %s AND A.institutionid  = B.institutionid;" \
                    %(self.userTable, self.bondTable, uid)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            kws = [x[0] for x in res if x[0] is not None]
            return kws
        except Exception,e:
            self.logger.error(str(e))
            self.logger.error("Fail to get fullname of bond by userID")       

    def getFullNamesOfCompanyByUserID(self, uid):
        try:
            sql = "SELECT DISTINCT B.fullname FROM %s A, %s B WHERE A.uid = %s AND A.institutionid  = B.institutionid;" \
                    %(self.userTable, self.companyTable, uid)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            kws = [x[0] for x in res if x[0] is not None]
            return kws
        except Exception,e:
            self.logger.error(str(e))
            self.logger.error("Fail to get fullname of company by userID")       

    def getShortNamesOfBondByUserID(self, uid):
        try:
            sql = "SELECT DISTINCT B.shortname FROM %s A, %s B WHERE A.uid = %s AND A.institutionid  = B.institutionid;" \
                    %(self.userTable, self.bondTable, uid)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            kws = [x[0] for x in res if x[0] is not None]
            return kws
        except Exception,e:
            self.logger.error(str(e))
            self.logger.error("Fail to get shortname of bond by userID")

    def getShortNamesOfCompanyByUserID(self, uid):
        try:
            sql = "SELECT DISTINCT B.shortname FROM %s A, %s B WHERE A.uid = %s AND A.institutionid  = B.institutionid;" \
                    %(self.userTable, self.companyTable, uid)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            kws = [x[0] for x in res if x[0] is not None]
            return kws
        except Exception,e:
            self.logger.error(str(e))
            self.logger.error("Fail to get shortname of company by userID")

    def getKeywordByUserID(self, uid):
        try:
            sql = "select distinct keyword from %s where uid=%s" %(self.userTable, uid)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            kws = [x[0] for x in res if x[0] is not None]
            return kws
        except Exception,e:
            self.logger.error(str(e))
            self.logger.error("Fail to get keyword by userID")
            
    def feedsPush(self, result):
        """result 数据格式list, 里面每一条结果为一个tuple。tuple中的元素顺序为(uid, notice_time, news_id, source_flag, score)
        """
        try:
            for res in result:
                # 判断在数据库中是否已经存在
                news_id = res[2]
                if news_id[0] == 'N':
                    sql_exists = "select * from %s where uid = %s and news_id = %s and source_flag = %s" % (self.correlationTable, res[0], news_id[1:], res[3])
                    self.cursor.execute(sql_exists)
                    res_exists = self.cursor.fetchall()

                    if len(res_exists) == 0:
                        sql = "insert into %s (uid, notice_time, news_id, source_flag, score, neg_sentiment) values" \
                               "(%s, '%s', %s, %s, %f, %f)" %(self.correlationTable, res[0], res[1], news_id[1:], res[3], res[4], res[5])
                        self.cursor.execute(sql)
                        self.logger.info("插入表 %s\t" % self.correlationTable + ' '.join([str(i) for i in res]))
            
            self.conn.commit()
        except Exception,e:
            self.logger.error(str(e))

    def createQuery(self, userID, dateStart, dateEnd):
        timequery =DateRange("notice_time", dateStart, dateEnd, startexcl=False, endexcl=False, boost=0)
        platformquery=Term("platform",1)
        platformquery.boost = 0
        keywords = self.getKeywordByUserID(userID)
        kwquery  = Or([ Term("content",x) for x in keywords])
        
        shortNameCom = self.getShortNamesOfCompanyByUserID(userID)
        shortNameCom = [name for name in shortNameCom if name not in self.shortNamesFilterSet]
        if not shortNameCom:
            shortNameComQuery = NullQuery()
        else:    
            shortNameComQuery = Or([Term("content",x) for x in shortNameCom])

        fullNameCom = self.getFullNamesOfCompanyByUserID(userID)
        fullNameComQuery = Or([Term("content",x) for x in fullNameCom])

        if len(keywords) > self.MAX_KEYWORDS:
            mainQuery = Or([shortNameComQuery, fullNameComQuery])
        else:
            shortNameBond = self.getShortNamesOfBondByUserID(userID)
            shortNameBondQuery = Or([Term("content",x) for x in shortNameBond])
            #fullNameBond = self.getFullNamesOfBondByUserID(userID)
            #fullNameBondQuery = Or([Term("content",x) for x in fullNameBond])
            mainQuery = Or([
                shortNameComQuery,
                shortNameBondQuery,
                fullNameComQuery,
                #fullNameBondQuery,
                ])

        kwquery.boost = 1.2
        query = AndMaybe(AndMaybe(timequery&platformquery, kwquery), mainQuery)
        return query
       
    def readFileCut(self, filename='CUT_WORDS_FILE'):
        filepath = self.dirpath + os.sep + 'data' + os.sep + filename
        lines = open(filepath).readlines()
        for line in lines:
            line_list = line.strip().split('||')
            
            res = {}
            user_name = unicode(line_list[0])
            res['key_word']   = [[unicode(x) for x in seg.split(' ')] for seg in line_list[1].split('\t')]
            res['short_name'] = [[unicode(x) for x in seg.split(' ')] for seg in line_list[2].split('\t')]
            res['short_name_bond'] = [[unicode(x) for x in seg.split(' ')] for seg in line_list[3].split('\t')]
            res['full_name']  = [[unicode(x) for x in seg.split(' ')] for seg in line_list[4].split('\t')]
            res['full_name_bond']  = [[unicode(x) for x in seg.split(' ')] for seg in line_list[5].split('\t')]
            
            self.user_words_cut[user_name] = res

    def searchFeedsByUserID(self, userID, dateStart, dateEnd):
        weighting = scoring.BM25F
        searcher = self.index.searcher(weighting=weighting)
        self.logger.info("user:%s "%(userID))
        query = self.createQuery(userID, dateStart, dateEnd)

        f1=ScoreFacet()
        f2=FieldFacet("sentiment",reverse=True)
        f3=FieldFacet("notice_time",reverse=True)
        facet = MultiFacet([f1,f2,f3])
        results = searcher.search(query, limit=1000, sortedby=facet, terms=True)

        insert_result = []
        for hit in results:
            #pdb.set_trace()
            title = hit.get('title', None).strip()
            url   = hit.get('url', None).strip()
            time  = hit.get('notice_time', None)
            matterms = hit.matched_terms()
            matwords = set()
            for tp in matterms:
                if tp[0] == 'content':
                    matwords.add(tp[1])
            matwordsStr = ' '.join(list(matwords))

            self.logger.info("搜索结果\t" + "%s\t%s\t%s\t%s\t%s\t%s" %(title, url, time, hit.score, hit.get('sentiment',None) , matwordsStr))
            #pdb.set_trace()
            news_id = hit.get('mysqlID', None)
            source_flag = hit.get('platform', None)
            score = abs(hit.score[0])
            sentiment = hit.get('sentiment',0.0)
            insert_result.append((userID, time, news_id,source_flag, score, sentiment))
            
        self.feedsPush(insert_result)

    def getUserID(self):
        try:
            sql = "select distinct(uid) from %s where uid is not null" %(self.userEmailTable)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            users = [x[0] for x in res]
            return users
        except Exception,e:
            self.logger.error(str(e))
            return []

    def searchFeeds(self, dateStart=None, dateEnd=None):
        logname = 'corr'+os.sep+'corr_'+datetime.datetime.now().strftime('%Y-%m-%d-%H') + '.log'
        self.logger = self._getLogger(logname)

        if dateStart is None or dateEnd is None:
            dateStart, dateEnd, am = self._getDatetimeRange(datetime.datetime.now())

        #self.readFileCut()
        userIDs = self.getUserID()
        for userID in userIDs:
            self.searchFeedsByUserID(userID, dateStart, dateEnd)

def runDaily():
    model = correlationRank()
    model.searchFeeds()

def runHistory():
    dateStart = datetime.datetime.strptime('201607111730','%Y%m%d%H%M%S')
    dateEnd   = datetime.datetime.strptime('201607120830','%Y%m%d%H%M%S')
    model = correlationRank()
    model.searchFeeds(dateStart, dateEnd)

if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
    #runDaily()
    runHistory()
