#coding:utf-8
#from __future__ import unicode_literals
import datetime
from whoosh.query import *
import numpy as np
import pdb
from newsIndex import publicOpinionBase
import os, sys

class HotTermFilter(object):
    def __init__(self):
        pass

    def filterNumber(self, items):
        res = []
        for key, val in items:
            if self.isNumber(key):
                continue
            res.append((key, val))
        return res

    def filterStopWords(self, items, path):
        stopfile = open(path).readlines()
        stopwords = set([x.strip() for x in stopfile])
        res = []
        for key, val in items:
            flag = True
            for stopword in stopwords:
                if stopword==key or key in stopword:
                    flag = False
            if flag: res.append((key, val))
        return res

    def isNumber(self, term):
        try:
            num = float(term)
            return True
        except:
            return False

class publicOpinionHotNewsRank(publicOpinionBase):

    def __init__(self):
        publicOpinionBase.__init__(self)
        self.hotTermLimit = 100
        self.keywordCount = 10000
        self.keytermsCount = 10000
        self.lookbackDays = 30
        self.hotTermsPerDatetimeFilePath = self.dirpath + os.sep + 'data'+ os.sep + 'hotTermsPerDatetimeFile'
        self.hotTermsPerDatetimeFile = open(self.hotTermsPerDatetimeFilePath, 'a') 
        self.hotStopWordsFilePath = self.dirpath + os.sep + 'data' + os.sep + 'STOPWORDS_HOT'
        self.__loadList()
        logname = 'hot'+os.sep+'hot_'+datetime.datetime.now().strftime('%Y-%m-%d-%H') + '.log'
        self.logger = self._getLogger(logname)
    
    def __loadList(self):
        self.blackList  = [x.strip() for x in open(self.dirpath + os.sep + 'data' + os.sep + 'BLACKLIST_HOT').readlines()]

        whiteFile = open(self.dirpath + os.sep + 'data' + os.sep + 'WHITELIST_HOT')
        self.whiteList = {}
        for line in whiteFile:
            line = line.strip()
            cols = line.split()
            key = cols[0]
            try:
                score = float(cols[1])
            except:
                score = 2.0
            self.whiteList[key] = score

    def __del__(self):
        self.hotTermsPerDatetimeFile.close()

    def hotTermPrepare(self, dateStart, dateEnd):
        
        searcher = self.index.searcher()
        timeQuery = DateRange("notice_time", dateStart, dateEnd)
        platformquery=Not(Term("platform",3) | Term("feed_type",2))
        srcInfo   = searcher.search(timeQuery & platformquery)
        #step1: get the documents ids of latest news
        docIDs_    = list(srcInfo.docs())
        docIDs   = []
        for doc in docIDs_:
            try:
                freq_vector = searcher.vector_as("frequency", doc, "content")
                freq_vector.next()
                docIDs.append(doc)
            except Exception, e:
                self.logger.debug(str(e))
        #step2: select some keywords
        termsInfo = searcher.key_terms(docIDs, "content", self.keytermsCount)
        terms     = set([ix[0] for ix in termsInfo])
        #terms = searcher.reader().most_distinctive_terms("content", number=self.keywordCount)
        #terms = set([unicode(ix[1]) for ix in terms])

        tScores     = {}
        tCountInDoc = {}
        tOccursInDocFreq = {}
        freqOfTermAndDoc = {}

        doc_len = float(len(docIDs))
        for doc in docIDs:
            try:
                freq_vector = searcher.vector_as("frequency", doc, "content")
                for term_freq in freq_vector:
                    term = term_freq[0]
                    freq = term_freq[1]
                    freqOfTermAndDoc[(doc, term)] = freq
                    if term not in tOccursInDocFreq:
                        tOccursInDocFreq[term] = 1
                    else:
                        tOccursInDocFreq[term] += 1

                    if doc not in tCountInDoc:
                        tCountInDoc[doc] = freq
                    else:
                        tCountInDoc[doc] += freq
            except Exception, e:
                self.logger.debug(str(e))

        for term in terms:
            sumScore = 0.0
            for doc in docIDs:
                #freq_vector = searcher.vector_as("frequency", doc, "content")
                #freq_dict = dict(freq_vector)
                try:
                    term_freq_in_doc = freqOfTermAndDoc[(doc, term)]
                    tf = term_freq_in_doc / float(tCountInDoc[doc])
                except:
                    tf = 0.0
                tf = np.sqrt(tf)
                idf = searcher.idf("content", term)
                try:
                    pdf = tOccursInDocFreq.get(term, 0) / doc_len 
                except:
                    pdf = 0.0
                score = tf * idf * pdf
                sumScore += score
            try:
                finalScore = sumScore / doc_len
            except:
                finalScore = 0.0
            if finalScore > 0:
                tScores[term] = finalScore
        termWeights = sorted(tScores.iteritems(),key=lambda x:x[1],reverse=True)
        model = HotTermFilter()
        termWeights = model.filterNumber(termWeights)
        termWeights = model.filterStopWords(termWeights, self.hotStopWordsFilePath)
        return termWeights
 


    def hotTermPrepare_pre(self, dateStart, dateEnd):
        
        searcher = self.index.searcher()
        keywordCount     = self.keywordCount
        timeQuery = DateRange("notice_time", dateStart, dateEnd)
        srcInfo   = searcher.search(timeQuery)

        #step1: get the documents ids of latest news
        docIDs    = list(srcInfo.docs())

        #step2: select some keywords
        #termsInfo = searcher.key_terms(docIDs, "content", keywordCount)
        #terms     = [ix[0] for ix in termsInfo]
        terms = searcher.reader().most_distinctive_terms("content", number=keywordCount)
        terms = [ix[1] for ix in terms]
        #step3: calculate the tfpdf scores for each term
        #indexedInverted = {}
        termScores      = {}
        termCountInDoc  = {}        # docnum : termcount
        termOccursInDocsFreq = {}   # term   : occurs freq in the lately documents
        for term in terms:
            matcher = searcher.reader().postings("content", term)
            termDic = dict(matcher.items_as("frequency"))
            nc = 0
            for docnum in docIDs:
                if termDic.get(docnum, 0)>0:
                    nc += 1
            try:
                freq = float(nc) / len(docIDs)
            except:
                freq = 0
            termOccursInDocsFreq[term] = freq
            for docnum, termfreq in termDic.items():
                if docnum in termCountInDoc:
                    termCountInDoc[docnum] += termfreq
                else:
                    termCountInDoc[docnum] = termfreq
        for term in terms:
            matcher = searcher.reader().postings("content", term)
            termDic = dict(matcher.items_as("frequency"))
            sumScore = 0.0
            for docnum in docIDs:
                tf_fenmu = termCountInDoc.get(docnum, 0.0)
                try:
                    tf = float(termDic.get(docnum, 0.0))/tf_fenmu
                except:
                    tf = 0.0
                tf = np.sqrt(tf)
                idf = searcher.idf("content", term)
                pdf = termOccursInDocsFreq[term]
                score = tf * idf * pdf
                #indexedInverted[term][docnum] = score
                sumScore += score
            try:
                finalScore = sumScore / len(docIDs)
            except:
                finalScore = 0.0
            if finalScore > 0:
                termScores[term] = finalScore
        termWeights = sorted(termScores.iteritems(),key=lambda x:x[1],reverse=True)
        #step4:filter 
        model = HotTermFilter()
        termWeights = model.filterNumber(termWeights)
        termWeights = model.filterStopWords(termWeights, self.hotStopWordsFilePath)

        return termWeights
        
    def writeFileFromTimeToTime(self, fromDate, toDate):
        
        fromDate_dt = datetime.datetime.strptime(fromDate, self.DATE_FORMAT) 
        toDate_dt = datetime.datetime.strptime(toDate, self.DATE_FORMAT) 
        delta_day = datetime.timedelta(days = 1)

        while fromDate_dt <= toDate_dt:
            # update morning and write
            update_morning_sdate = (fromDate_dt - delta_day).strftime(self.DATE_FORMAT)
            update_morning_stime = update_morning_sdate + ' ' + self.updateNight
            update_morning_start = datetime.datetime.strptime(update_morning_stime, self.TIME_FORMAT)
            update_morning_edate = fromDate_dt.strftime(self.DATE_FORMAT)
            update_morning_etime = update_morning_edate + ' ' + self.updateMorning
            update_morning_end   = datetime.datetime.strptime(update_morning_etime, self.TIME_FORMAT)
            
            morning_res = self.hotTermPrepare(update_morning_start, update_morning_end)
            self.hotTermsPerDatetimeFile.write(update_morning_etime + '\t')
            #pdb.set_trace()
            strInfo = ' '.join(["%s:%.06f" %(x[0], x[1]) for x in morning_res])
            self.hotTermsPerDatetimeFile.write("%s"%strInfo)
            self.hotTermsPerDatetimeFile.write('\n')
            
            # logger
            self.logger.info("%s\t%s\t%s" % (update_morning_etime,u"词语分数计算", u"前五词：") + ' '.join(["%s:%.06f" %(x[0],x[1]) for x in morning_res[:5]]))

            # update night and write
            update_night_etime = update_morning_edate + ' ' + self.updateNight
            update_night_end   = datetime.datetime.strptime(update_night_etime, self.TIME_FORMAT)
            
            night_res = self.hotTermPrepare(update_morning_end, update_night_end)
            self.hotTermsPerDatetimeFile.write(update_night_etime + '\t')
            self.hotTermsPerDatetimeFile.write(' '.join(["%s:%.06f" %(x[0], x[1]) for x in night_res]))
            self.hotTermsPerDatetimeFile.write('\n')

            # logger
            self.logger.info("%s\t%s\t%s" % (update_night_etime, u"词语分数计算", u"前五词：") + ' '.join(["%s:%.06f" %(x[0],x[1]) for x in night_res[:5]]))
            
            fromDate_dt += delta_day
            
    def writeFileDaily(self, now = datetime.datetime.now()):
        table = open(self.hotTermsPerDatetimeFilePath).readlines()
        line = table[len(table) - 1]
        cols = line.strip().split('\t')
        date_str = cols[0]
        
        morning_checktime_str = now.strftime(self.DATE_FORMAT) + ' ' + self.updateMorning
        morning_checktime     = datetime.datetime.strptime(morning_checktime_str, self.TIME_FORMAT)
        
        # update PM
        if now > morning_checktime:
            update_night_etime = now.strftime(self.DATE_FORMAT) + ' ' + self.updateNight
            update_night_time  = datetime.datetime.strptime(update_night_etime, self.TIME_FORMAT)
            
            if update_night_etime > date_str:
                res = self.hotTermPrepare(morning_checktime-datetime.timedelta(seconds=2400), update_night_time)
                self.hotTermsPerDatetimeFile.write(update_night_etime + '\t')
                self.hotTermsPerDatetimeFile.write(' '.join(["%s:%.06f" % (x[0], x[1]) for x in res]))
                self.hotTermsPerDatetimeFile.write('\n')

                # logger
                self.logger.info("%s\t%s\t%s" % (update_night_etime, u"词语分数计算", u"前五词：") + ' '.join(["%s:%.06f" %(x[0],x[1]) for x in res[:5]]))
        # update AM
        else:
            delta_day = datetime.timedelta(days = 1)
            yesterday_str = (now - delta_day).strftime(self.DATE_FORMAT)
            yesterday_start_str = yesterday_str + ' ' + self.updateNight
            yesterday_start = datetime.datetime.strptime(yesterday_start_str, self.TIME_FORMAT)
            if morning_checktime_str > date_str:
                res = self.hotTermPrepare(yesterday_start- datetime.timedelta(seconds=2400), morning_checktime)
                self.hotTermsPerDatetimeFile.write(morning_checktime_str + '\t')
                self.hotTermsPerDatetimeFile.write(' '.join(["%s:%.06f" % (x[0], x[1]) for x in res]))
                self.hotTermsPerDatetimeFile.write('\n')

                # logger
                self.logger.info("%s\t%s\t%s" % (morning_checktime_str, u"词语分数计算", u"前五词：") + ' '.join(["%s:%.06f" %(x[0],x[1]) for x in res[:5]]))

    def hotTermsDetector(self, date, am=True):
        table = open(self.hotTermsPerDatetimeFilePath).readlines()
        lookbackDays = self.lookbackDays
        if am:
            datekey = date.strftime('%Y-%m-%d ')+self.updateMorning
        else:
            datekey = date.strftime('%Y-%m-%d ')+self.updateNight
        series = None
        for ix in range(len(table)-1, -1, -1):
            line = table[ix]
            cols = line.strip().split('\t')
            datestr = cols[0]
            if datekey==datestr:
                sloc = ix - lookbackDays if(ix-lookbackDays>=0) else 0
                series = table[sloc:ix+1] 
                break
        if series is None:return
        termDic = {}
        line = series[-1]
        cols = line.strip().split('\t')
        termScore = cols[1].split(' ')
        for key in termScore:
          try:
            items = key.split(':')
            term  = items[0]
            score = float(items[1])
            termDic[term] = [ score ]
          except:
            pass
        
        for line in series[::-1][1:]:
            cols = line.strip().split('\t')
            termScore = cols[1].split(' ')
            termDicDate   = {}
            for key in termScore:
                items = key.split(':')
                term  = items[0]
                score = float(items[1])
                termDicDate[term] = score
            for term in termDic:
                if term in termDicDate:
                    if termDicDate[term]>0:
                        termDic[term].append(termDicDate[term])
                else:
                    termDic[term].append(0.0)
                    #pass
        termHotWeights = {}
        for term in termDic:
            scores  = termDic[term]
            current = scores[0]
            past    = np.mean(scores[1:])
            if np.isnan(past) and current!=0:
                weight = 3.0
            elif current==0: weight = current
            elif past==0: weight = 3.0
            else:weight = float(current) / past
            #if np.std(scores)==0:
            #    weight = 0
            #else:
            #    weight  = ( current - np.mean(scores) ) / np.std(scores)
            #    weight  = weight if weight>0 else 0
            termHotWeights[term] = weight
        termHotWeights = sorted(termHotWeights.iteritems(),key=lambda x:x[1],reverse=True)
        model = HotTermFilter()
        termHotWeights = model.filterNumber(termHotWeights )
        termHotWeights = model.filterStopWords(termHotWeights , self.hotStopWordsFilePath)

        return termHotWeights[:self.hotTermLimit]

    def feedsPush(self, result):
        """result 数据格式list,list中每一条记录包含news_id和hot值的信息
        """
        for res in result:
            try:
                hot = res['score']
                platform = res['mysqlID'][0]
                news_id = res['mysqlID'][1:]
                if platform=='N':
                    sql = "update %s set hot = %f where news_id = %s" %(self.tableNews, hot, news_id)
                elif platform=='W':
                    sql = "update %s set hot = %f where id = %s" %(self.tableWeixin, hot, news_id)

                self.cursor.execute(sql)
                self.logger.info("Update record mysqlID:%s" %res['mysqlID'])
            except Exception, e:
                self.logger.error("更新hot出错\t" + "出错记录news_id = %s hot = %f" %(news_id, hot) + "\t" + str(e))

        self.conn.commit()

    def docRankByHots(self, dateStart, dateEnd, am=True): 

        termWeights = self.hotTermsDetector(dateEnd, am)

        printInfo = ''
        for term, score in termWeights:
            printInfo += '%s:%s\t'%(term, score)

        self.logger.info("Top 100 words %s"%printInfo)
            
        searcher  = self.index.searcher()
        timeQuery = DateRange("notice_time", dateStart, dateEnd)

        platformquery=Not(Term("platform",3) | Term("feed_type",2))
        srcInfo   = searcher.search(timeQuery&platformquery, limit=1000)
        docIDs    = list(srcInfo.docs())
        docWeights = dict([(x , 0.0) for x in docIDs])
        #计算文档的热度分数值：目前由热度词分数求和得到
        termWeightsDic = dict(termWeights)
        for doc in docIDs:
            try:
                freqVector = searcher.vector_as('frequency', doc, "content")
                for item in freqVector:
                    term = item[0]  #unicode
                    freq = item[1]
                    if freq>0: 
                        val = termWeightsDic.get(term.encode('utf8'), 0.0)
                        docWeights[doc]+=val
            except Exception,e:
                self.logger.debug(str(e))

        #for term, val in termWeights:
        #    matcher = searcher.reader().postings("content", term)
        #    termDic = dict(matcher.items_as("frequency"))
        #    for doc in docIDs:
        #        if termDic.get(doc, 0)>0:
        #            docWeights[doc] += val
        #获取由docnum到hit的映射
        idToDic = {}
        for hit in srcInfo:
            idToDic[hit.docnum] = dict(hit.iteritems())
        res = []
        #将热度分数归一化, 并和新闻负情绪分数相加
        docWeights_ = self.weightsNorm(docWeights.items())
        docWeights_ = [(x[0], x[1]+idToDic[x[0]]['sentiment']) for x in docWeights_ ]

        #进行白名单和黑名单过滤
        docWeightsFinal = []
        for line in docWeights_:
            docnum = line[0]
            weight = line[1]
            dic    = idToDic[docnum]
            title = dic['title']
            whitescore = self.filterByWhiteList(title)
            if whitescore!=0:
                weight = whitescore
            if self.filterByBlackList(title):
                weight = -1.0
            docWeightsFinal.append((docnum, weight)) 
        #结果排序并输出
        docWeightsFinal= sorted(docWeightsFinal, key=lambda x:x[1] , reverse=True)
        for line in docWeightsFinal:
            docnum = line[0]
            weight = line[1]
            dic    = idToDic[docnum]
            title = dic['title']
            dic['docnum'] = docnum
            dic['score'] = weight
            dic['hotscore'] = docWeights[docnum]
            res.append(dic)
            self.logger.info("%.02f\t%s\t%s\t%s\t%.02f\t%.02f" %(dic['score'], dic['title'], dic['notice_time'] ,dic['url'], dic['sentiment'], dic['hotscore']))

        return res

    def filterByBlackList(self, key):
        for word in self.blackList:
            if word in key:
                return True
        return False

    def filterByWhiteList(self, key):
        for word in self.whiteList:
            if word in key:
                return self.whiteList[word]
        return 0

    def weightsNorm(self, tuples):
        weights = [x[1] for x in tuples]
        maxWeight = np.max(weights)
        if maxWeight==0:maxWeight=1
        res = []
        for x in tuples:
            res.append((x[0], x[1]/maxWeight))
        return res

    def runDaily(self,dateStart=None, dateEnd=None, am=True):
        if dateStart is None and dateEnd is None:
            dateStart, dateEnd, am = self._getDatetimeRange(datetime.datetime.now())
        self.writeFileDaily(dateEnd)
        results = self.docRankByHots(dateStart, dateEnd, am)
        self.feedsPush(results)

def runDaily():
    news_rank = publicOpinionHotNewsRank()
    news_rank.runDaily()

def runHistory():
    news_rank = publicOpinionHotNewsRank()
    #news_rank.writeFileFromTimeToTime('2016-06-01', '2016-07-03')
    #news_rank.writeFileDaily(datetime.datetime.strptime('201607040840','%Y%m%d%H%M'))
    news_rank.runDaily(datetime.datetime.strptime('201607111700','%Y%m%d%H%M'),\
                       datetime.datetime.strptime('201607120830','%Y%m%d%H%M'))

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    #runHistory()
    runDaily()
