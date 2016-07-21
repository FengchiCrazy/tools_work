#coding:utf-8

from dbConnect import dataBase
import sys, os
import jieba
import jieba.analyse 
import datetime
import MySQLdb
import pdb
reload(sys)
sys.setdefaultencoding('utf8')

dirpath = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])
filepath = dirpath + os.sep + 'data' + os.sep + "shortNamesForLoading.dic"
jieba.load_userdict(filepath)

STOPWORDS = ['\r','\n','\t',' ','\r\n']
MYWORDS   = [
             u'有限公司',
             u'公司',
             u'有限责任',
             ]

class queryPrepare(dataBase):
    
    def __init__(self):
        dataBase.__init__(self)
        logname = 'query'+os.sep+'query_'+datetime.datetime.now().strftime('%Y-%m-%d-%H') + '.log'
        self.logger = self._getLogger(logname)
        self.__createStopList()

    def __createStopList(self, filename='STOPWORDS'):
        filepath = self.dirpath + os.sep + 'data' + os.sep + filename
        
        all_words = open(filepath).readlines()
        self.stoplist = set([word.strip() for word in all_words] + STOPWORDS + MYWORDS)
        
        

    def tokenFilter(self ,token):
        token = token.lower()
        return token

    def getUserName(self,uid):
        username_sql = "select user_name from news_subscibe where uid = %s " % uid
        self.cursor.execute(username_sql)
        username_res = self.cursor.fetchall()
        try:
            un_ret = username_res[0][0]
        except:
            un_ret = None
        return un_ret

    def getCompaniesAndBunds(self, uid):
        inst_sql = "select distinct institutionid from news_subscibe where uid = %s order by institutionid " % uid
        self.cursor.execute(inst_sql)
        inst_res = self.cursor.fetchall()
        inst_res = [x[0] for x in inst_res if x is not None]
        
        keyword_sql = "select distinct key_word from news_subscibe where uid = %s " % uid
        self.cursor.execute(keyword_sql)
        keyword_res = self.cursor.fetchall()
        # use for utf8
        keyword_list = [x[0] for x in keyword_res if x is not None and x[0] is not None]
        ret = []
        #print inst_res
        for inst in inst_res:
            item = {'company':{}, 'bond':[]}
            if inst is None:
                continue
            company_sql = "select institutionid, shortname, fullname " \
                "from pub_institutioninfo where institutionid = %s" % inst
            self.cursor.execute(company_sql)
            try:
                company = self.cursor.fetchall()[0]
                # use for utf8
                item['company']['institutionid']   = company[0]
                item['company']['shortname']       = company[1]
                item['company']['fullname']        = company[2]
            except:
                self.logger.error("ERROR in pub_institutioninfo " + str(inst))
                continue
            
            bond_sql = "select shortname, fullname from bond_info where institutionid = %s " % inst
            self.cursor.execute(bond_sql)
            try:
                bonds = self.cursor.fetchall()
                for bond in bonds:
                    bond_dic = {}
                    # use for utf8
                    bond_dic['shortname'] = bond[0]
                    bond_dic['fullname']  = bond[1]
                    item['bond'].append(bond_dic)
            except:
                self.logger.error("ERROR in bond_info where institutionid = %s" % inst)
                continue
            ret.append(item)
            
        return ret, keyword_list

    def cutWordsFromList(self, words):
        if len(words)==0:
            return ''

        res = ''
        for word in words:
            tokens = []
            gene = jieba.cut(word, cut_all=False)
            for token in gene:
                if token not in self.stoplist:
                    tokens.append(self.tokenFilter(token))
            res += '%s\t' %(' '.join(tokens))
        res = res[:-1]
        return res

    def extractWordsFromList(self, words, topN=3):
        if len(words)==0:
            return ''
        res = ''
        for word in words:
            tokens = []
            gene = jieba.analyse.extract_tags(word,topN)
            for token in gene:
                tokens.append(self.tokenFilter(token))
            res += '%s\t' %(' '.join(tokens))
        res = res[:-1]
        return res

    def createCutWords(self, uid=1):
        info , kws = self.getCompaniesAndBunds(uid)
        short_name = []
        short_name_bond = []
        full_name = []
        short_full_name = []
        full_name_bond = []
        short_full_name_bond = []
        for item in info:
            insshortname = item['company']['shortname']
            insfullname  = item['company']['fullname']
            sfname = ''
            if insshortname is not None : 
                short_name.append(insshortname)
                sfname += insshortname
            if insfullname  is not None : 
                full_name.append(insfullname)
                sfname += insfullname
            short_full_name.append(sfname)
            bonditems = item['bond']
            for line in bonditems:
                #line {'fullname':, 'shortname':}
                bondshortname= line['shortname']
                bondsfullname= line['fullname']
                sfbond = ''
                if bondshortname is not None :
                    short_name_bond.append(bondshortname)
                    sfbond += bondshortname
                if bondsfullname is not None :
                    full_name_bond.append(bondsfullname)
                    sfbond += bondsfullname
                short_full_name_bond.append(sfbond)
        short_name_str = self.joinWordsFromList(short_name) 
        short_name_bond_str = self.joinWordsFromList(short_name_bond)
        key_word_str = self.joinWordsFromList(kws)
        #deal full name
        full_name_str = self.cutWordsFromList(short_full_name)
        full_name_bond_str = self.cutWordsFromList(full_name_bond)
        return key_word_str,short_name_str,short_name_bond_str,full_name_str,full_name_bond_str

    def joinWordsFromList(self, words):
        return '\t'.join(words)

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

    def createCutFile(self, filename = 'CUT_WORDS_FILE'):
        filepath = self.dirpath + os.sep + 'data' + os.sep + filename

        fw = open(filepath, 'w')
        userIDs = self.getUserID() 
        
        username_set = set()
        for userID in userIDs:
            self.logger.info('userID = %s started!' % userID)
            username = self.getUserName(userID) 
            if username in username_set:
                continue
            else:
                username_set.add(username)
                res = self.createCutWords(userID) 
                fw.write(username + '||')
                fw.write('||'.join(res))
                fw.write('\n')

        fw.close()
            
    
if __name__ == '__main__':
    model = queryPrepare() 
    model.createCutFile()
