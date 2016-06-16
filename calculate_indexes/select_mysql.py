#-*- coding:utf-8 -*-
import MySQLdb
import sys
import pdb
from decimal import Decimal

class SelectMySQL(object):
    """
    Base class: get the select results from MySQL
    """
    def __init__(self):
        self.__host = 'localhost'
        self.__user = 'root'
        self.__passwd = 'passwd'
        self.__db = 'db'
        self.__table = 'table'
        self.__conn = MySQLdb.connect(
            host=self.__host,
            user=self.__user,
            passwd=self.__passwd,
            db=self.__db
        )
        self.__cursor = self.__conn.cursor()

    def __del__(self):
        self.__cursor.close()
        self.__conn.close()

    def executeSelectMySQL(self, sql):
        self.__cursor.execute(sql)
        return self.__cursor.fetchall()

    def getListOfTheDay(self, date):
        sql = "select distinct SECURITYCODE from FUND_NV_NV where CHANGEDATE='%s'" % date
        ret = self.executeSelectMySQL(sql)
        return [x[0] for x in ret]

    def getListOfFund(self, category):
        sql = "select distinct SECURITYCODE from FUND_BS_OFINFO where FPROPERTY = '%s'" % category
        ret = self.executeSelectMySQL(sql)
        return [x[0] for x in ret]

    def getListFundOfDay(self, category):
        day_fund  = self.getListOfTheDay(self.date)
        fund = self.getListOfFund(category)
        return list(set(day_fund) & set(fund))

    def getCompanyName(self, code):
        sql = "SELECT B.COMPANYCODE,C.COMPANYNAME FROM CDSY_SECUCODE AS B JOIN ORGA_BI_ORGBASEINFO AS C ON C.COMPANYCODE = B.COMPANYCODE"\
              " WHERE B.SECURITYCODE = '%s' AND B.SECURITYTYPECODE LIKE '059%%'  LIMIT 1" % (code)
        ret = self.executeSelectMySQL(sql)
        try:
            ret = ret[0][1]
            #ret = ret.decode(sys.stdout.encoding)
            return ret
        except:
            return code
    
    def getPerNavList(self, code, fromDate, toDate):
        """
        if fromDate is None, means get the PERNAV list from the vary begninning.
        """
        sql = "select PERNAV from FUND_NV_NV where SECURITYCODE='%s' and CHANGEDATE <= '%s' and PERNAV is not null " % (code, toDate)
        if fromDate is not None:
            sql += "and CHANGEDATE >= '%s' " % fromDate
        sql += "order by CHANGEDATE"
        ret = self.executeSelectMySQL(sql)
            
        return [x[0] for x in ret]

    def getReturnSeriesList(self, code, fromDate, toDate):
        """
        if fromDate is None, means get the PERNAV list from the vary begninning.
        """
        sql = "select WANPERNAV, PERNAV from FUND_NV_NV where SECURITYCODE='%s' and CHANGEDATE <= '%s' and WANPERNAV is not null and PERNAV is not null " % (code, toDate) 
        if fromDate is not None:
            sql += "and CHANGEDATE >= '%s' " % fromDate
        sql += "order by CHANGEDATE"
        ret = self.executeSelectMySQL(sql)
        try:
            if isinstance(ret[0][0], Decimal):
                return [x[0] / Decimal(10000.0) / x[1] for x in ret]
            else:
                return [x[0] / 10000.0 / x[1] for x in ret]
        except:
            return None





if __name__ == "__main__":
    sc = SelectClass()
    x = sc.getPerNav('001282', '2016-03-01', '2016-05-01')
    print x    
    
        
