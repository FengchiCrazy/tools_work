#coding:utf-8
import sys, os
import pymssql
import logging
import pdb
import MySQLdb
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

def to_str(string):
    if isinstance(string, unicode):
        value = string.encode('utf8')
    else:
        value = string
    return value

def to_unicode(string):
    if isinstance(string, str):
        value = string.decode('utf8')
    else:
        value = string
    return value

class dataBase(object):

    def __init__(self):
        conn = pymssql.connect(
            server = '',
            port   = "",
            user   = '',
            password = '',
            database = '',
            charset= 'utf8'
        )
        cursor = conn.cursor()
        self.conn = conn
        self.cursor = cursor
        dirpath        = os.path.split(os.path.realpath(__file__))[0]
        self.dirpath   = os.path.dirname(dirpath)
        
        self.mysql_conn, self.mysql_cursor = self._getMySQL()
        self.weeklyNewsTable = 'news_weekly_stats_info'

    def _getLogger(self, filename):
        level = logging.DEBUG
        file_path = filename
        formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

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

    def __del__(self):
        self.cursor.close()
        self.conn.close()
        
        self.mysql_cursor.close()
        #self.mysql_conn.close()


    def _getMySQL(self):
        conn2 = MySQLdb.connect(
            host   = '',
            user   = '',
            passwd = '',
            db     = '',
            charset= ''
        )   
        cursor2 = conn2.cursor()
        conn2.set_character_set('utf8')
        cursor2.execute('SET NAMES utf8;')
        cursor2.execute('SET CHARACTER SET utf8;')
        cursor2.execute('SET character_set_connection=utf8;')
        
        return conn2, cursor2

    def checkIfExistsInWeeklyTable(self, institutionid, dateobj):
        sql = "select count(*) from %s where institutionid=%s and report_date='%s'"\
            % (self.weeklyNewsTable, institutionid, dateobj.strftime('%Y-%m-%d'))
        self.mysql_cursor.execute(sql)
        res = self.mysql_cursor.fetchall()[0][0]
        if res == 0:
            return False
        else:
            return True
    
    def handleName(self, name):
        if name != '':
            name = "'" + name + "'"
        else:
            name = 'null'
        return name

if __name__ == '__main__':
    model = dataBase()
    sql   = """
select DISTINCT INDUSTRYCODE, INDUSTRYNAME from PUB_INDCLASSIFYSETS where INDCLASSIFYSYSTEMCODE='P0207' and RANK =2;
    """ 
    model.cursor.execute(sql)
    res = model.cursor.fetchall()
    for i in range(1,29):
        print "SW%s=" %str(i) 
