import ConfigParser
import pdb
import datetime
import sys
import os

from mssqlConn import dataBase

class ParseInstAndNews(dataBase):
    def __init__(self, filepath, datestring):
        dataBase.__init__(self)
        self.inst_news_dict = self._parseConfig(filepath)
        datestring = datestring[:8]
        self.date  = datetime.datetime.strptime(datestring, '%Y%m%d')

        logpath = self.dirpath + os.sep + 'log' + os.sep + 'negNews_%s.log' % self.date.strftime('%Y_%m_%d')
        self.logger = self._getLogger(logpath)
        
    def _parseConfig(self, filepath):
        config = ConfigParser.ConfigParser()
        config.read(filepath)

        # dict: institutionid : newsID
        return dict(config.items('general'))

    def parseConfigIntoMysql(self):
        for inst, newsID in self.inst_news_dict.items():
            flag = self.checkIfExistsInWeeklyTable(inst, self.date)
            if flag:
                sql = "update %s set neg_top_flag = %s, neg_news_id = %s where institutionid = %s "\
                    " and report_date='%s'" % (self.weeklyNewsTable, '1', newsID, inst, self.date.strftime('%Y-%m-%d'))
                self.logger.info('UPDATE institutionid:%s\tneg_top_flag:%s\tneg_news_id:%s' % (inst, '1', newsID))
            else:
                sql = "insert %s (institutionid, report_date, neg_top_flag, neg_news_id) values(%s, '%s', %s, %s)"\
                    %(self.weeklyNewsTable, inst, self.date.strftime('%Y-%m-%d'), '1', newsID)
                self.logger.info('INSERT institutionid:%s\tneg_top_flag:%s\tneg_news_id:%s' % (inst, '1', newsID))
            self.mysql_cursor.execute(sql)
        self.mysql_conn.commit()
        self.mysql_conn.close()
                
if __name__ == '__main__':
    filepath = sys.argv[1]
    datestring = sys.argv[2]
    model = ParseInstAndNews(filepath, datestring)
    model.parseConfigIntoMysql()
