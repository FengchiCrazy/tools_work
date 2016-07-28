#coding:utf-8
import sys, os
import datetime
import pdb
import logging
from mssqlConn import dataBase, to_str, to_unicode
import ConfigParser
import math

reload(sys)
sys.setdefaultencoding('utf8')

class industry(dataBase):

    def __init__(self, path, configpath,datestring):
        dataBase.__init__(self)
        datestring = datestring[:8]
        self.date = datetime.datetime.strptime(datestring, '%Y%m%d')

        loggerpath = self.dirpath + os.sep + 'log' + os.sep + 'industry_%s.log' % self.date.strftime('%Y_%m_%d')
        self.logger = self._getLogger(loggerpath)
        self.config_path = configpath

        self._loadNegativeIns(path)
        self._loadIndTransFile()

    def _loadNegativeIns(self, path):
        # negative dict institutionid : (news_count_neg, shortname, fullname)
        self.negativeDict = {}
        insfile = open(path)
        for line in insfile:
            try:
                line = line.strip().split('\t')
                institutionid  = int(line[0])
                shortname      = self.handleName(line[1])
                fullname       = self.handleName(line[2])
                news_count_neg = int(line[4])
                if not institutionid in self.negativeDict:
                    self.negativeDict[institutionid] = (news_count_neg, shortname, fullname)
            except:
                continue

    def __dealLowerCase(self, items):
        res = [(x[0].upper(),x[1].upper()) for x in items]
        return res
    
    def _loadIndTransFile(self):
        config = ConfigParser.ConfigParser()
        config.read(self.config_path)

        #base industry dic
        self.baseIndDic = dict(self.__dealLowerCase(config.items('base')))
        #shenwan industry dic
        self.newIndDic = dict(self.__dealLowerCase(config.items('shenwan')))
        #industry trans
        self.base2new  = dict(self.__dealLowerCase(config.items('transform')))

    def _getIndDic(self):
        sql = "select DISTINCT INDUSTRYCODE, INDUSTRYNAME from PUB_INDCLASSIFYSETS where INDCLASSIFYSYSTEMCODE='P0207' and RANK=2"
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        dic = {}
        for line in res:
            code = line[0]
            name = line[1]
            if code is None or name is None:
                continue
            dic[code] = name
        return dic

    def _getCompanyIndustryDic(self):
        #A.ShortName , A.FullName , A.IndustryCode, D.INDUSTRYCODE, D.INDUSTRYNAME
        self.name2code = {}
        sql = """
select DISTINCT A.InstitutionID, A.ShortName , C.INDUSTRYCODE, C.INDUSTRYNAME
from STK_InstitutionInfo A LEFT JOIN STK_IndustryClass B ON (A.IndustryCode = B.IndustryCode and B.IndustryClassificationID='P0207')
INNER JOIN PUB_INDCLASSIFYSETS C  ON (B.IndustryCode = C.INDUSTRYCODE and B.IndustryClassificationID=C.INDCLASSIFYSYSTEMCODE);
"""
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        dic = {}
        for line in res:
            try:
              insID = int(line[0]) 
              shortname = line[1]
              indCode = line[2]
              self.name2code[insID] = shortname
            except:
              continue
            indCode = self.base2new.get(indCode, None)
            if indCode is None:
                continue
            dic[insID] = indCode
        return dic

    def stats(self):
        #industryDic = self._getIndDic()
        com2ind     = self._getCompanyIndustryDic()
        ind2com = {}
        for com , ind in com2ind.items():
            if com in self.negativeDict:
                self.feedsPush(com, ind, self.newIndDic[ind])
            if ind in ind2com:
                ind2com[ind].add(com)
            else:
                ind2com[ind] = set([com])

        self.mysql_conn.commit()
        self.mysql_conn.close()

        for ind in ind2com:
            insDisSet = ind2com[ind] & set(self.negativeDict.keys() )
            comInfo = ','.join( ["%s:%s:%s"%(x , self.name2code.get(x,''), self.negativeDict[x][0]) for x in insDisSet])
            total = sum([self.negativeDict[x][0] for x in insDisSet])
            self.logger.critical("%s:%s\t%s\t%s" %(ind,self.newIndDic[ind], math.log(total+1.0), comInfo))

    def feedsPush(self, institutionid, industry, industry_name):
        flag = self.checkIfExistsInWeeklyTable(institutionid, self.date)
        if flag:
            sql = "update %s set industry = '%s', industry_name = '%s', news_count_neg = %s where institutionid = %s "\
                " and report_date = '%s'" % (self.weeklyNewsTable, industry, to_str(industry_name),\
                 self.negativeDict[institutionid][0], institutionid, self.date.strftime('%Y-%m-%d'))
            self.logger.info('UPDATE institutionid:%s\tindustry:%s\tindustry_name:%s' % (institutionid, industry, industry_name))
        else:
            sql = "insert %s (institutionid, report_date, industry, industry_name, news_count_neg, shortname, fullname)" \
                "values (%s, '%s', '%s', '%s', %s, %s, %s)" % (self.weeklyNewsTable, institutionid, \
                self.date.strftime('%Y-%m-%d'), industry, to_str(industry_name), \
                self.negativeDict[institutionid][0], self.negativeDict[institutionid][1], self.negativeDict[institutionid][2])
            self.logger.info('INSERT institutionid:%s\tindustry:%s\tindustry_name:%s' % (institutionid, industry, industry_name))
        self.mysql_cursor.execute(sql)
            
if __name__ == '__main__':
    path = sys.argv[1]
    configpath = sys.argv[2]
    datestring = sys.argv[3]
    model = industry(path, configpath,datestring)
    model.stats()
