import datetime
import os
import sys

from mssqlConn import dataBase, to_str, to_unicode

class MediaAttentionUpload(dataBase):
    def __init__(self, filepath, datestring):
        dataBase.__init__(self)
        self.media_attention_dict = self._getMediaAttentionFile(filepath)
        datestring = datestring[:8]
        self.date = datetime.datetime.strptime(datestring, '%Y%m%d')
        
        logpath = self.dirpath + os.sep + 'log' + os.sep + 'media_%s.log' % self.date.strftime('%Y_%m_%d')
        self.logger = self._getLogger(logpath)
    
    def _getMediaAttentionFile(self, filepath):
        # media_attention_dict institutionid : (attention_number, shortname, fullname)
        ret = {}
        with open(filepath, 'r') as f:
            for line_ in f:
                line = line_.strip().split('\t')
                institutionid = line[0]
                shortname     = self.handleName(line[1])
                fullname      = self.handleName(line[2])
                attention_number = line[3]
            
                ret[institutionid] = (attention_number, shortname, fullname)

        return ret

    def stats(self):
        for inst, values in self.media_attention_dict.items():
            attention_number = values[0]
            shortname        = values[1]
            fullname         = values[2]

            flag = self.checkIfExistsInWeeklyTable(inst, self.date)
            if flag:
                sql = "update %s set news_count = %s where institutionid = %s and report_date = '%s'"\
                    % (self.weeklyNewsTable, attention_number, inst, self.date.strftime('%Y-%m-%d')) 
                self.logger.info('UPDATE institutionid:%s\tattention_number:%s' % (inst, attention_number))
            else:
                sql = "insert %s (institutionid, report_date, news_count, shortname, fullname) values(%s, '%s', %s, %s, %s) "\
                    % (self.weeklyNewsTable, inst, self.date.strftime('%Y-%m-%d'), attention_number,shortname, fullname)
                self.logger.info('INSERT institutionid:%s\tattention_number:%s' % (inst, attention_number))
            self.mysql_cursor.execute(sql)
        
        self.mysql_conn.commit()

if __name__ == '__main__':
    filepath = sys.argv[1]
    datestring = sys.argv[2]
    model = MediaAttentionUpload(filepath, datestring)
    model.stats()

                 
