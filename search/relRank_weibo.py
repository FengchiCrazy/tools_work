# coding:utf-8

#from __future__ import unicode_literals
from relRank import correlationRank
import pdb
import whoosh.scoring as scoring
from whoosh.sorting import *
import datetime
from whoosh.query import *
import pickle
import os

class correlationRankWeibo(correlationRank):
    def __init__(self):
        correlationRank.__init__(self)

    def feedsPush(self, result):
        """result 数据格式list, 里面每一条结果为一个tuple。tuple中的元素顺序为(uid, notice_time, news_id, source_flag, score)
        """
        try:
            for res in result:
                # 判断在数据库中是否已经存在
                news_id = res[2]
                #pdb.set_trace()
                if news_id[0] == 'B':
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
        platformquery=Term("platform",3)
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
            url   = hit.get('url','').strip()
            time  = hit.get('notice_time', None)
            matterms = hit.matched_terms()
            matwords = set()
            for tp in matterms:
                if tp[0] == 'content':
                    matwords.add(tp[1])
            matwordsStr = ' '.join(list(matwords))

            self.logger.info("搜索结果\t" + "%s\t%s\t%s\t%s" %(url, time, hit.score, matwordsStr))
            #pdb.set_trace()
            news_id = hit.get('mysqlID', None)
            source_flag = hit.get('platform', 3)
            score = abs(hit.score[0])
            sentiment = hit.get('sentiment',0.0)
            insert_result.append((userID, time, news_id,source_flag, score, sentiment))
            
        self.feedsPush(insert_result)

    def searchFeeds(self, dateStart=None, dateEnd=None):
        logname = 'corr_weibo'+os.sep+'corr_'+datetime.datetime.now().strftime('%Y-%m-%d-%H') + '.log'
        self.logger = self._getLogger(logname)

        if dateStart is None or dateEnd is None:
            dateStart, dateEnd, am = self._getDatetimeRange(datetime.datetime.now())

        #self.readFileCut()
        userIDs = self.getUserID()
        for userID in userIDs:
            self.searchFeedsByUserID(userID, dateStart, dateEnd)

def runDaily():
    model = correlationRankWeibo()
    model.searchFeeds()

def runHistory():
    dateStart = datetime.datetime.strptime('201607011730','%Y%m%d%H%M%S')
    dateEnd   = datetime.datetime.strptime('201607120830','%Y%m%d%H%M%S')
    model = correlationRankWeibo()
    model.searchFeeds(dateStart, dateEnd)

if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
    #runDaily()
    runHistory()
