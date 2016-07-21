import os
from dbConnect import dataBase

    
class testSegWords(dataBase):
    def __init__(self):
        dataBase.__init__(self)
    
    def createShortNames(self, filename="shortNamesForLoading.dic"):
        filepath = self.dirpath + os.sep + 'data' + os.sep + filename
        fw = open(filepath, 'w')
        
        sql_fond_sn = "select distinct shortname from bond_info where shortname is not null"

        self.cursor.execute(sql_fond_sn)
        fond_short_name = self.cursor.fetchall()
        
        fw.write('\n'.join([ "%s"%x[0].encode('utf8').strip() for x in fond_short_name]))
        fw.write('\n')

        sql_company_sn = "select distinct shortname from pub_institutioninfo where shortname is not null"
        self.cursor.execute(sql_company_sn)
        company_short_name = self.cursor.fetchall()
        
        fw.write('\n'.join(["%s"%x[0].encode('utf8').strip() for x in company_short_name]))

        fw.close()

    
if __name__ == '__main__':
    model = testSegWords()
    model.createShortNames()
