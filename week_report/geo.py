#coding:utf-8
import sys, os
import pymssql
import logging
import pdb
import math
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
from collections import defaultdict
from mssqlConn import dataBase, to_str, to_unicode

class CompanyProvince(dataBase):

    def __init__(self, filepath, datestring):
        dataBase.__init__(self)
        datestring = datestring[:8]
        self.date = datetime.datetime.strptime(datestring, '%Y%m%d')
        
        self.filter_area_name=set([
            u'市辖区',
            u'县',
            u'自治区直辖县级行政区划',
        ])
        self.proAreaSum = defaultdict(list)

        # counter
        self.listedCount = 0
        self.matchingAddressCount = 0
        self.noMatchingAddressCount = 0
        self.noAddressCount = 0
        self.errorCount = 0
        
       
        # get logger
        logPath = self.dirpath + os.sep + 'log' + os.sep + 'geo_%s.log' % self.date.strftime("%Y_%m_%d")
        self.logger = self._getLogger(logPath)
        
        # load file
        self._loadIndTransFile(filepath)

    def _loadIndTransFile(self, path):
        # negative dict institutionid : (news_count_neg, shortname, fullname)
        self.negativeDict = {}
        insfile = open(path)
        for line in insfile:
            try:
                line = line.strip().split('\t')
                institutionid  = line[0]
                shortname      = line[1]
                fullname       = line[2]
                news_count_neg = int(line[4])
                if not institutionid in self.negativeDict:
                    self.negativeDict[institutionid] = (news_count_neg, shortname, fullname)
            except:
                continue
    
    def createProAreaDict(self):
        sql = "select DIVISIONCODE, DIVISIONNAME from PUB_CHNADMDIVISIONCODE order by DIVISIONCODE"
        self.cursor.execute(sql)
        res = self.cursor.fetchall() 
        self.proAreaDict = defaultdict(list)
        for divisioncode, divisionname in res:
            if divisioncode[2:] == '0000':
                divisionname_ = divisionname[:2]
                if divisionname_ == '内蒙' or divisionname_ == '黑龙':
                    divisionname_ = divisionname[:3]
                self.proAreaDict[divisioncode].append(divisionname_)
            elif divisioncode[4:] != '00':
                continue
            else:
                division_pro = divisioncode[:2] + '0000'
                if divisionname in self.filter_area_name:
                    continue
                divisionname = divisionname[:2]
                if divisionname == u'': continue
                self.proAreaDict[division_pro].append(divisionname) 
        self.logger.debug('Province Area dict completed!')

    def countProvinceNumberFull(self):
        self.logger.debug('start!')
        self.createProAreaDict()
        listedInstitutionProvinceDict = self.createListedProDict()
        institutionidList = list(self.negativeDict.keys())
        self.total_count = len(institutionidList)
        for inst in institutionidList:
            if inst in listedInstitutionProvinceDict:
                provincecode = listedInstitutionProvinceDict[inst]
                self.proAreaSum[provincecode].append(inst)
                self.feedsPush(inst, provincecode, self.proAreaDict[provincecode][0])

                self.listedCount += 1
                continue
                
            sql = "select registeraddress, officeaddress, institutionid from pub_institutioninfo where institutionid = %s" % inst
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            try:
                registeraddress = res[0][0]
                officeaddress   = res[0][1]
            except Exception, e:
                pdb.set_trace()
                self.logger.error('insitutionid:%s\tError:%s' % (inst, str(e)))
                self.errorCount += 1
                continue

            if registeraddress:
                self.countProvince(registeraddress, inst)
            elif officeaddress:
                self.countProvince(officeaddress, inst)
            else:
                self.logger.info('institutionid:%s\tHas No address' % inst)
                self.noAddressCount += 1
                continue

        self.mysql_conn.commit()
        self.mysql_conn.close()

        self.loggerResults()

    def loggerResults(self):
        self.logger.critical('total institution:%d' % self.total_count)
        self.logger.critical('listed count:%d' % self.listedCount)
        self.logger.critical('matching address count:%d' % self.matchingAddressCount)
        self.logger.critical('no matching address count:%d' % self.noMatchingAddressCount)
        self.logger.critical('no address count:%d' % self.noAddressCount)
        self.logger.critical('error count:%d' % self.errorCount)

        for divisioncode, inst_list in self.proAreaSum.items():
            total_count_neg = sum([self.negativeDict[inst][0] for inst in inst_list])
            log_count_neg = math.log(1 + float(total_count_neg))
            
            inst_string = ' '.join('%s:%s:%d' % (inst, self.negativeDict[inst][1], self.negativeDict[inst][0]) for inst in inst_list)
            
            self.logger.critical('%s:%s\t%f\t%s' % (divisioncode, to_str(self.proAreaDict[divisioncode][0]), log_count_neg, inst_string))
            

        
        
    
    def countProvince(self, address, institutionid):
        def matching_area(address, institutionid):
            for divisioncode, proAreaList in self.proAreaDict.items():
                for proArea in proAreaList:
                    if proArea in address[:6]:
                        # logger
                        self.proAreaSum[divisioncode].append(institutionid)
                        self.matchingAddressCount += 1 
                        self.logger.info("institutionid:%s\tMATCHING: %s\t%s" %\
                            (institutionid, proArea.encode('utf8'), address.encode('utf8')))

                        self.feedsPush(institutionid, divisioncode, self.proAreaDict[divisioncode][0])
                        return 
            self.noMatchingAddressCount += 1
            self.logger.info('Cannot find institutionid:%s\taddress:%s' %(institutionid, address))
                
        matching_area(address, institutionid)

    def createListedProDict(self):
        sql = "select A.institutionid, A.ProvinceCode from STK_InstitutionInfo A"
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        listedInstitutionProvince = {}
        for institutionid, provincecode in res:
            if institutionid is not None and provincecode is not None:
                listedInstitutionProvince[institutionid] = provincecode
        
        self.logger.debug('Listed province dict completed!')

        return listedInstitutionProvince

    def feedsPush(self, institutionid, location, location_name):
        flag = self.checkIfExistsInWeeklyTable(institutionid, self.date)
        if flag:

            sql = "update %s set location = '%s', location_name = '%s', news_count_neg = %s where institutionid = %s and "\
                "report_date = '%s'" %(self.weeklyNewsTable, to_str(location), to_str(location_name), \
                self.negativeDict[institutionid][0], institutionid, self.date.strftime('%Y-%m-%d'))
            self.logger.info('UPDATE institutionid:%s\tlocation:%s\tlocation_name:%s' % (institutionid, location, location_name))
        else:
            sql = "insert %s (institutionid, report_date, location, location_name, news_count_neg, shortname, fullname)"\
                " values (%s, '%s', '%s', '%s', %s, %s, %s)" % (self.weeklyNewsTable, institutionid, \
                self.date.strftime('%Y-%m-%d'), to_str(location), to_str(location_name),\
                self.negativeDict[institutionid][0],self.handleName(self.negativeDict[institutionid][1]), self.handleName(self.negativeDict[institutionid][2]))
            self.logger.info('INSERT institutionid:%s\tlocation:%s\tlocation_name:%s' % (institutionid, location, location_name))

        self.mysql_cursor.execute(sql)
        

if __name__ == '__main__':
    path = sys.argv[1]
    datestring = sys.argv[2]
    model = CompanyProvince(path, datestring)
    model.countProvinceNumberFull()

