#-*- coding:utf-8 -*-

import MySQLdb
import pdb
import os
import datetime
from pprint import pprint

conn = MySQLdb.connect(
        host   = '127.0.0.1',
        user   = 'root',
        passwd = '',
        db     = 'test',
        charset= 'utf8'
        )   
cursor = conn.cursor()
conn.set_character_set('utf8')
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')

today = datetime.datetime.now().strftime('%Y%m%d')

renren_dict = {
    'fengchi':'峰池',
}

def create_news_subscribe_csv(temppath='temp'):
    sql_username = "select distinct user_name from news_subscibe"
    cursor.execute(sql_username)
    res_username = cursor.fetchall()
    res_username = [un[0] for un in res_username]
    
    if not os.path.exists(temppath):
        os.mkdir(temppath)

    for i, user_name in enumerate(res_username):
        sql_kw = "select distinct key_word, symbol, institutionid from news_subscibe where user_name = '%s'" %user_name.encode('utf8')
        cursor.execute(sql_kw)
        res_kw = cursor.fetchall() 
        
        f_kw = open('%s%s%s_keyword_%s.csv' % (temppath, os.sep, user_name.encode('utf8'), today), 'w')
        
        if len(res_kw) == 1 and res_kw[0][0] is None and res_kw[0][1] is None and res_kw[0][2] is None:
            print user_name.encode('utf8') + ' wocao kong!'
        else:   
            contents = []
            for res in res_kw:
                if None in res:
                    print user_name.encode('utf8'), res, res_kw
                key_word = res[0].encode('utf8') if res[0] is not None else ''
                symbol = res[1].encode('utf8') if res[1] is not None else ''
                institutionid = str(res[2]) if res[2] is not None else ''
                if '公司' in key_word or '研究院' in key_word:
                    symbol = ''
                    type_ = '0'
                else:
                    type_ = '2'
                contents.append((key_word, type_, symbol, institutionid)) 

            contents = sorted(list(set(contents)))
            
            for content in contents:
                f_kw.write('ADD,' + ','.join(content) + '\n')
            
        f_kw.close()
        
        sql_email = "select distinct email from news_subscibe where user_name = '%s'" %user_name.encode('utf8')
        cursor.execute(sql_email)
        res_email = cursor.fetchall() 
        f_email = open('%s%s%s_email_%s.csv' % (temppath, os.sep, user_name.encode('utf8'), today), 'w')
        
        user_set = set()
        for res in res_email:
            email = res[0].encode('utf8') if res[0] is not None else ''
            reciptionist = email.split('@')[0]
            domain_name = email.split('@')[1].split('.')[0]
            if domain_name != 'renren-inc':
                type_ = 'user'
                user_set.add(email)
            else:
                try:
                    reciptionist = renren_dict[reciptionist]
                except:
                    print reciptionist
                type_ = 'renren'
            
            f_email.write('ADD,%s,%s,%s\n' %(reciptionist ,email, type_))
        f_email.close()

        f_manager = open('%s%s%s_manager_%s.csv' % (temppath, os.sep, user_name.encode('utf8'), today), 'w')
        f_manager.write('ADD,2\n')
        f_manager.close()




if __name__ == '__main__':
    create_news_subscribe_csv()
