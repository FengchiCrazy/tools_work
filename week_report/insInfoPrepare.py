#coding:utf-8
import sys, os
import datetime
import pdb
import logging
from mssqlConn import dataBase

reload(sys)
sys.setdefaultencoding('utf8')

class insInfo(dataBase):

    def __init__(self, path):
        dataBase.__init__(self)
        self.fileNum = 1000
        self.path = path

    def _deal_punctuation(self , line):
        line = line.replace('（', '')
        line = line.replace('）','')
        line = line.replace(')','')
        line = line.replace('(','')
        return line

    def createFiles(self):
        fileName = self.path + os.sep + 'ins_file_'
        sql = "select count(DISTINCT InstitutionID) from PUB_InstitutionInfo where CategoryID like 'P041[4-5]' or CategoryID='P0499'"
        self.cursor.execute(sql)
        total = self.cursor.fetchall()[0][0]
        disp = int(total/ float(self.fileNum))
        wordDic = {}
        sql = "select InstitutionID, ShortName, FullName from PUB_InstitutionInfo where CategoryID like 'P041[4-5]' or CategoryID='P0499'"
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        lastFileCnt = -1
        for ix, line in enumerate(res):
            filecnt = ix/disp
            if filecnt != lastFileCnt:
                if lastFileCnt>-1:objFile.close()
                objFile = open(fileName+str(filecnt), 'w')
                lastFileCnt = filecnt
            insID = line[0]
            shortName = line[1] if line[1] is not None else ''
            shortName = self._deal_punctuation(shortName)
            fullName = line[2] if line[2] is not None else ''
            fullName = self._deal_punctuation(fullName)
            if shortName == '' and fullName == '':continue
            objFile.write("%s\t%s\t%s\n"%(insID, shortName, fullName))

if __name__ == '__main__':
    comfilepath = sys.argv[1]
    model = insInfo(comfilepath)
    model.createFiles()
            
