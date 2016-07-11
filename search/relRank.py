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
        self.logger = self._getLogger('CORR.log')
        

    def getCompaniesAndBunds(self, uid):
        inst_sql = "select distinct institutionid from news_subscibe where uid = %s order by institutionid " % uid
        self.cursor.execute(inst_sql)
        inst_res = self.cursor.fetchall()
        inst_res = [x[0] for x in inst_res if x is not None]
        
        keyword_sql = "select distinct key_word from news_subscibe where uid = %s " % uid
        self.cursor.execute(keyword_sql)
        keyword_res = self.cursor.fetchall()
        # use for utf8
        #keyword_list = [unicode(x[0]) for x in keyword_res if x is not None]
        keyword_list = [x[0] for x in keyword_res if x is not None and x[0] is not None]
        
        ret = []
    
        #print inst_res
        for inst in inst_res:
            item = {'company':{}, 'bond':[]}
            if inst is None:
                continue
            company_sql = "select institutionid, shortname, fullname, category, registeraddress, officeaddress, mainbusiness, businessscope " \
                "from pub_institutioninfo where institutionid = %s" % inst
            self.cursor.execute(company_sql)
            try:
                company = self.cursor.fetchall()[0]
                # use for utf8
                #item['company']['shortname']       = unicode(company[1])
                #item['company']['fullname']        = unicode(company[2])
                #item['company']['category']        = unicode(company[3])
                #item['company']['registeraddress'] = unicode(company[4])
                #item['company']['officeaddress']   = unicode(company[5])
                #item['company']['mainbusiness']    = unicode(company[6])
                #item['company']['businessscope']   = unicode(company[7])

                item['company']['shortname']       = None if company[1] is None else company[1]
                item['company']['fullname']        = None if company[2] is None else company[2]
                item['company']['category']        = None if company[3] is None else company[3]
                item['company']['registeraddress'] = None if company[4] is None else company[4]
                item['company']['officeaddress']   = None if company[5] is None else company[5]
                item['company']['mainbusiness']    = None if company[6] is None else company[6]
                item['company']['businessscope']   = None if company[7] is None else company[7]
            except:
                print "ERROR in pub_institutioninfo " + str(inst)
                continue
            
            bond_sql = "select shortname, fullname from bond_info where institutionid = %s " % inst
            self.cursor.execute(bond_sql)
            try:
                bonds = self.cursor.fetchall()
                for bond in bonds:
                    bond_dic = {}
                    # use for utf8
                    #bond_dic['shortname'] = unicode(bond[0])
                    #bond_dic['fullname']  = unicode(bond[1])

                    bond_dic['shortname'] = None if bond[0] is None else bond[0]
                    bond_dic['fullname']  = None if bond[1] is None else bond[1]

                    item['bond'].append(bond_dic)
            except:
                print "ERROR in bond_info where institutionid = %s" % inst
                continue

            ret.append(item)
            
        return ret, keyword_list
            
    def feedsPush(self, result):
        """result 数据格式list, 里面每一条结果为一个tuple。tuple中的元素顺序为(uid, notice_time, news_id, source_flag, score)
        """
        try:
            for res in result:
                # 判断在数据库中是否已经存在
                news_id = res[2]
                if news_id[0] == 'N':
                    sql_exists = "select * from news_correlation where uid = %s and news_id = %s and source_flag = %s" % (res[0], news_id[1:], res[3])
                    self.cursor.execute(sql_exists)
                    res_exists = self.cursor.fetchall()

                    if len(res_exists) == 0:
                        sql = "insert into news_correlation (uid, notice_time, news_id, source_flag, score, neg_sentiment) values" \
                               "(%s, '%s', %s, %s, %f, %f)" %(res[0], res[1], news_id[1:], res[3], res[4], res[5])
                        self.cursor.execute(sql)
                        self.logger.info("插入表 news_correlation\t" + ' '.join([str(i) for i in res]))
            
            self.conn.commit()
        except Exception,e:
            self.logger.error(str(e))

    def createQueryByKeywords(self, info, keywords, dateStart, dateEnd):
        '''
        info      : list 
        keywords  : list of keywords
        '''
        #step1:timequery 
        #timequeryStr = "notice_time:>='%s' notice_time:<='%s'" %(dateStart.strftime('%Y%m%d%H%M%S'),\
        #                                                         dateEnd.strftime('%Y%m%d%H%M%S'))
        #timequery    = self.parse_search(timequeryStr)
        timequery =DateRange("notice_time", dateStart, dateEnd, startexcl=False, endexcl=False, boost=0)
        platformquery=self.parse_search("platform:1")
        platformquery.boost = 0

        #step2:keyword query
        
        #kwqueryStrPart = '~|'.join(keywords) + '~'
        #kwqueryStr = "main_body:%s | product_info:%s | content:%s" %(kwqueryStrPart,kwqueryStrPart,kwqueryStrPart)
        #kwquery_    = self.parse_search(kwqueryStr)
        kwqueryStrPart = '|'.join(keywords)
        kwqueryStr = "main_body:%s" %(kwqueryStrPart)
        kwquery    = self.parse_search(kwqueryStr)
        #step3:main query
        companys = []
        bonds    = []
        business = []
        location = []
        for item in info:
            bonditems    = item['bond']    #list
            comShortName = item['company']['shortname']     
            #comFullName  = item['company']['fullname']          
            #comCate      = item['company']['category']           
            #comAdd       = item['company']['registeraddress']
            #comCadd      = item['company']['officeaddress']  
            #comBus       = item['company']['mainbusiness']  
            #comBusc      = item['company']['businessscope']  
            if comShortName is not None :companys.append(comShortName)
            #if comFullName is not None :companys.append(comFullName)
            #if comBus is not None :business.append(comBus)
            #if comBusc is not None :business.append(comBusc)
            #if comAdd is not None :location.append(comAdd)
            #if comCadd is not None :location.append(comCadd)
            for line in bonditems:
                #line {'fullname':, 'shortname':}
                #bondfullname = line['fullname'] 
                bondshortname= line['shortname']
                #if bondfullname is not None : bonds.append(bondfullname)
                if bondshortname is not None :bonds.append(bondshortname)

        companyqueryStr = '|'.join([x.decode('utf8') for x in companys])
        bondsqueryStr = '|'.join([x.decode('utf8') for x in  bonds])
        #businessqueryStr = '|'.join([x.decode('utf8') for x in business])
        #locationqueryStr = '|'.join( [x.decode('utf8') for x in location])
        
        contentQueryStr = "content:%s|%s" %(companyqueryStr, bondsqueryStr)    
        contentQuery    = self.parse_search(contentQueryStr)
        #mainbodyQueryStr = "main_body:%s" %companyqueryStr
        #mainbodyQuery    = self.parse_search(mainbodyQueryStr)
        #productQueryStr  = "product_info:%s" %(bondsqueryStr)
        #productQuery     = self.parse_search(productQueryStr)

        mainQuery = contentQuery

        #query combine
        kwquery.boost = 1.2
        query = AndMaybe(AndMaybe(timequery&platformquery, kwquery), mainQuery)
        return query

    def searchFeedsByUserID(self, usrID, dateStart, dateEnd):
        weighting = scoring.BM25F
        searcher = self.index.searcher(weighting=weighting)

        #file_path = self.dirpath + os.sep + 'data' + os.sep + 'pickle' + os.sep
        #pickleFileName = "%spickle_%s"%(file_path, usrID)
        #if os.path.exists(pickleFileName):
        #    cp = pickle.load(open(pickleFileName))
        #    info = cp[0]
        #    kws = cp[1]
        #else:
        #    pickle.dump([info, kws], open(pickleFileName,'w'))
        info, kws = self.getCompaniesAndBunds(usrID)
        query     = self.createQueryByKeywords(info, kws, dateStart, dateEnd)
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
            insert_result.append((usrID, time, news_id,source_flag, score, sentiment))
            
        self.feedsPush(insert_result)

    def getUserID(self):
        try:
            sql = "select distinct(uid) from  news_subscibe where uid<9800"
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            users = [x[0] for x in res]
            return users
        except Exception,e:
            self.logger.error(str(e))
            return []

    def searchFeeds(self, dateStart=None, dateEnd=None):
        if dateStart is None or dateEnd is None:
            dateStart, dateEnd, am = self._getDatetimeRange(datetime.datetime.now())

        userIDs = self.getUserID()
        for userID in userIDs:
            self.searchFeedsByUserID(userID, dateStart, dateEnd)

def runDaily():
    model = correlationRank()
    model.searchFeeds()

def runHistory():
    dateStart = datetime.datetime.strptime('201607050730','%Y%m%d%H%M%S')
    dateEnd   = datetime.datetime.strptime('201607051730','%Y%m%d%H%M%S')
    model = correlationRank()
    model.searchFeeds(dateStart, dateEnd)

if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf8')
    runDaily()
    #runHistory()
