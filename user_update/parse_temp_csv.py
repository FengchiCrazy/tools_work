#-*- coding:utf-8 -*-

import MySQLdb
import pdb
import os
import datetime
import logging
from pprint import pprint


ADD_OPERATION = 'ADD'
DEL_OPERATION = 'DEL'

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

def create_log(log_name='ERROR.log', console_log=True, level=logging.DEBUG):
    formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)s - %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

    logger = logging.getLogger()
    logger.setLevel(level)
    
    if log_name:
        fh = logging.FileHandler(log_name, mode = 'a')
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if console_log:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    
    return logger

log_name = 'log' + os.sep + 'Error_' + datetime.datetime.now().strftime('%Y%m%d') + '.log'
logger = create_log(log_name = log_name)

def init_data_csv(datapath='data'):
    """初始化data目录下的文件，查询news_subscibe表中存在的user_name根据指定的filetype创立data目录下的空文件。

       一次性使用函数。
    """
    if not os.path.exists(datapath):
        os.mkdir(datapath)
     
    file_types = [
        'keyword',
        'email',
        'manager',
    ]

    sql_username = "select distinct user_name from news_subscibe"
    cursor.execute(sql_username)
    res_username = cursor.fetchall()
    res_username = [un[0] for un in res_username]
    
    for i, user_name in enumerate(res_username):
        user_name_utf = user_name.encode('utf8')
        for file_type in file_types:
            f_kw = open('%s%s%04d_%s_%s.csv' % (datapath, os.sep, i + 1, user_name_utf, file_type), 'w')
            f_kw.close()
        
        

def parse_temp_to_data(temppath='temp', datapath='data', errorpath='error'):
    """将temp文件夹下的数据读入，添加进mysql并把记录写入data文件夹下的记录中。并会把写入记录时的异常情况写入log

       temp文件夹下会有三种文件类型，分为keyword, email, manager，在文档中分别有相应的命名格式。如果文件命名有误，会跳过这一文件，并写入log

    """
    username_id_dict = {}
    file_types = [
        'keyword',
        'email',
        'manager',
    ]

    temp_file_list = os.listdir(temppath)
    data_file_list = os.listdir(datapath)

    if not os.path.exists(errorpath):
        os.mkdir(errorpath)

    if len(temp_file_list) == 0:
        raise ValueError("There must be csv files under '%s' folder" %temppath)

    for data_file in data_file_list:
        df_split  = data_file.split('.csv')[0].split('_')
        uid       = df_split[0]
        user_name = df_split[1]

        if user_name not in username_id_dict:
            username_id_dict[user_name] = int(uid)

    for temp_file in temp_file_list:
        tf = open("%s%s%s" %(temppath, os.sep, temp_file), 'r')
        tf_contents = tf.readlines()

        tf_split = temp_file.split('.csv')[0].split('_')
        user_name    = tf_split[0]
        file_type    = tf_split[1]
        date         = tf_split[2]

        # if temp file's type is wrong, copy it to error folder
        if file_type not in file_types:
            logger.info('temp/%s 文件命名错误，请检查。该文件内容将会被跳过' % temp_file)
            open('%s%s%s'% (errorpath, os.sep, temp_file), 'w').write(tf.read())
            continue
        
        # find the data file of temp file. If not, create new file
        flag = False
        for data_file in data_file_list:
            if user_name in data_file and file_type in data_file:
                flag = True
                break

        if flag:
            df = open("%s%s%s" % (datapath, os.sep, data_file), 'a')
        else:
            # judge if user_name has uid. If user_name doesn't has uid, the new uid will be created as auto increment
            if user_name not in username_id_dict:
                max_uid = max(username_id_dict.values())
                uid_ = max_uid + 1
                username_id_dict[user_name] = uid_
            else:
                uid_ = username_id_dict[user_name]
            df  = open("%s%s%04d_%s_%s.csv" % (datapath, os.sep, uid_, user_name, file_type), 'a')

        # insert temp files into mysql, create error file
        uid = username_id_dict[user_name]
        error_contents = []
        rt_contents = []
        if file_type == 'email':
            rt_contents, error_contents = handle_email_table(uid, user_name, tf_contents)
        elif file_type == 'keyword':
            rt_contents, error_contents = handle_keyword_table(uid, user_name, tf_contents)
        elif file_type == 'manager':
            rt_contents, error_contents = handle_manager_table(uid, user_name, tf_contents)

        # if has error_contents, write in error_file
        if len(error_contents) > 0:
            error_file = open('%s%s%s' % (errorpath, os.sep, temp_file), 'a')
            for content in error_contents:
                error_file.write(content + '\n')
            error_file.close()

        # write file in data
        if len(rt_contents) > 0:
            for content in rt_contents:
                df.write(date + ',' + content.strip() + '\n')

        df.close()
        tf.close()

def handle_email_table(uid, user_name, contents):
    """处理email型文件增删news_user_email表

       return (rt_contents, error_contents)
               rt_contents：list. 排除掉出错记录之后，原始记录外加查找到的信息,元素为字符串，依次为'operation,recipient,email,type'
               error_contents：list. 原始记录出错后将会把该条记录外加出错原因加入到error_contents中，元素为字符串，依次为'operation,recipient,email,type,error_reason'
    """
    rt_contents = []    
    error_contents = []

    for line_ in contents:
        line = line_.strip().split(',')
        operation = line[0].upper()
        recipient = line[1]
        email     = line[2]
        if len(line) >= 4:
            type_ = line[3]
        else:
            type_ = 'user'

        if operation == ADD_OPERATION:
            sql = "insert into news_user_email (uid, username, recipient, email, type) values"\
                  "(%s, '%s', '%s', '%s', '%s')" % (uid, user_name, recipient, email, type_)
            try:
                cursor.execute(sql)
            except Exception, e:
                logger.error(sql)
                logger.error(str(e))
                error_contents.append(line_.strip() + ',' + str(e))
                continue
        elif operation == DEL_OPERATION:
            sql = "delete from news_user_email where uid = %s and email = '%s' " % (uid, email)
            try:
                cursor.execute(sql)
            except Exception, e:
                logger.info(sql)
                logger.info(str(e))
                error_contents.append(line_.strip() + ',' + str(e))
                continue
        else:
            logger.info('Error Operation-%s of username:%s' % (operation, user_name))
            error_contents.append(line_.strip() + ',Error operation')
            continue

        rt_contents.append(line_.strip())

    #conn.commit()
    return rt_contents, error_contents

def handle_keyword_table(uid, user_name, contents):
    """处理keyword型文件增删news_user_keyword表。根据keyword记录中type值，0代表公司，1代表股票，2代表债券，分成三个子逻辑进行处理。
       对于公司，securityid设置为0，根据keyword的名称与pub_institutioninfo表中的shortname和fullname进行匹配，取到institutionid
       对于债券，根据债券的symbol值域bond_info表中的symbol值进行匹配,对于可能出现的多个bond对应同一个symbol值情况，确保keyword与bond的shortname相同

       股票逻辑暂缺!!!!

       如果上述查找出现问题导致没有查找到，会把该条记录外加错与原因输出到error文件夹下,并打出log


       return (rt_contents, error_contents)
               rt_contents：list. 排除掉出错记录之后，原始记录外加查找到的信息,元素为字符串，依次为'operation,key_word,type_,symbol,securityid,res_institutionid'
               error_contents：list. 原始记录出错后将会把该条记录外加出错原因加入到error_contents中，元素为字符串，依次为'operation,key_word,type_,symbol,error_reason'
               
       
    """
    rt_contents = []    
    error_contents = []

    for line_ in contents:
        line = line_.strip().split(',')
        operation     = line[0].upper()
        key_word      = line[1]
        type_         = line[2]
        # ensure the csv has enough ',' if keyword is all company
        if len(line) >= 4:
            symbol    = line[3] if line[3] is not '' else 'null'
        else:
            symbol    = 'null'

        # will not occour in the regular file but could be used as double check of the record
        # Especially useful when checking the record from old database
        check_flag = False
        if len(line) >= 5:
            check_flag    = True
            institutionid = line[4]
        else:
            institutionid = 0

        if type_ == '2':
            sql_secid = "select securityid, shortname,institutionid, symbol from bond_info where symbol = '%s'" % symbol 
            cursor.execute(sql_secid)
            res_secid = cursor.fetchall()
            if len(res_secid) == 1:
                securityid = res_secid[0][0].encode('utf8')
                res_institutionid = res_secid[0][2]
            else:
                securityid = None 
                res_institutionid = -1
                for res in res_secid:
                    if res[1] == key_word.decode('utf8'):
                        securityid = res[0].encode('utf8')
                        res_institutionid = res[2]
                        #pdb.set_trace()
                        #logger.info('%s: %s, %s, %s, %s, %s' % (user_name, key_word, symbol, securityid, res_institutionid, institutionid))
                        break
                if res_institutionid == -1:
                    logger.error('CANNOT find the institutionid of symbol ("%s") of %s! Please double check the symbol!' % (symbol, user_name))
                    error_contents.append(line_.strip() + ',Error bond info')
                    continue

        elif type_ == '0':
            securityid = '0'
            sql_inst_fullname = "select fullname, shortname, institutionid from pub_institutioninfo where fullname = '%s'" % key_word.decode('utf8')
            cursor.execute(sql_inst_fullname)
            res_fullname = cursor.fetchall()   
            #pdb.set_trace()
            if len(res_fullname) == 0:
                sql_inst_shortname = "select fullname, shortname, institutionid from pub_institutioninfo where shortname = '%s'" % key_word.decode('utf8')
                cursor.execute(sql_inst_shortname)
                res_shortname = cursor.fetchall()
                if len(res_shortname) == 0:
                    res_institutionid = -1
                else:
                    res_institutionid = res_shortname[0][2] if res_shortname[0][2] is not None else -1
            else:
                res_institutionid = res_fullname[0][2] if res_fullname[0][2] is not None else -1

            if res_institutionid == -1:
                logger.error('CANNOT find the institutionid of keyword ("%s") of %s! Please double check the keyword!' % (key_word, user_name)) 
                error_contents.append(line_.strip() + ',Error company keyword!')
                continue
        else:
            # todo: deal with stock which type_ = '1'
            pass

        if check_flag and res_institutionid != int(institutionid):
            #logger.warning('different institutionid--%s: %s, %s, %s, %s, %s' % (user_name, key_word, symbol, securityid, res_institutionid, institutionid))
            pass

        institutionid = res_institutionid

        if operation == ADD_OPERATION:
    
            if symbol == 'null':
                sql = "insert into news_user_keyword (uid, username, keyword, type, symbol, securityid, institutionid) "\
                     "values (%s, '%s', '%s', %s, %s, '%s', %s)" %(uid, user_name, key_word, type_, symbol, securityid, institutionid)
            else:
                sql = "insert into news_user_keyword (uid, username, keyword, type, symbol, securityid, institutionid) "\
                     "values (%s, '%s', '%s', %s, '%s', '%s', %s)" %(uid, user_name, key_word, type_, symbol, securityid, institutionid)
            try:
                cursor.execute(sql)
            except Exception, e:
                logger.error(sql)
                logger.error(str(e))
                error_contents.append(line_.strip() + ',' + str(e))
                continue

        elif operation == DEL_OPERATION:
            sql = "delete from news_user_keyword where uid = %s and keyword = '%s' and securityid = '%s'" % (uid, key_word.decode('utf8'), securityid)
            try:
                cursor.execute(sql)
            except Exception, e:
                logger.error(sql)
                logger.error(str(e))
                error_contents.append(line_.strip() + ',' + str(e))
                continue
        else:
            logger.info('Error Operation-%s of username:%s' % (operation, user_name))
            error_contents.append(line_.strip() + ',Error operation')
            continue

        rt_contents.append(','.join([operation, key_word, type_, symbol, securityid, str(res_institutionid)]))

    #conn.commit()
    return rt_contents, error_contents

def handle_manager_table(uid, user_name, contents):
    """处理manager型文件增删news_user_manager表

       return (rt_contents, error_contents)
               rt_contents：list. 排除掉出错记录之后，原始记录外加查找到的信息,元素为字符串，依次为'operation,managerid'
               error_contents：list. 原始记录出错后将会把该条记录外加出错原因加入到error_contents中，元素为字符串，依次为'operation,managerid,error_reason'
    """

    rt_contents = []    
    error_contents = []

    for line_ in contents:
        line = line_.strip().split(',')
        operation = line[0].upper()
        is_managerid = True
        try:
            managerid = int(line[1])
        except:
            is_managerid = False
            manager_name = line[1]

        if is_managerid == False:
            sql_mid = "select id, name from b2_user_assist_manager where name = '%s'" % manager_name
            cursor.execute(sql_mid)
            res_mid = cursor.fetchall()
            if len(res_mid) > 0 and res_mid[0][0] is not None:
                managerid = res_mid[0][0]
            else:
                logger.error('Error manager name-%s of username:%s' % (manager_name, user_name))
                error_contents.append(line_.strip() + ',Error manager name')
                continue
        else:
            sql_mn = "select id, name from b2_user_assist_manager where id = %s" % managerid 
            cursor.execute(sql_mn)
            res_mn = cursor.fetchall()
            if len(res_mn) > 0 and res_mn[0][1] is not None:
                manager_name = res_mn[0][1].encode('utf8')
            else:
                logger.error('Error manager id-%s of username:%s' % (managerid, user_name))
                error_contents.append(line_.strip() + ',Error manager id')
                continue
             

        if operation == ADD_OPERATION:
            sql = "insert into news_user_manager (uid, username, managerid) values( %s, '%s', %s)" % (uid, user_name, managerid)
            try:
                cursor.execute(sql)
            except Exception, e:
                logger.info(sql)
                logger.info(str(e))
                error_contents.append(line_.strip() + ',' + str(e))
                continue

        elif operation == DEL_OPERATION:
            sql = "delete from news_user_manager where uid = %s and managerid = %s " % (uid, managerid)
            try:
                cursor.execute(sql)
            except Exception, e:
                logger.info(sql)
                logger.info(str(e))
                error_contents.append(line_.strip() + ',' + str(e))
                continue
        else:
            logger.info('Error Operation-%s of username:%s' % (operation, user_name))
            error_contents.append(line_.strip() + ',Error operation')
            continue

        rt_contents.append(','.join((operation, str(managerid), manager_name)))

    conn.commit()
    return rt_contents, error_contents

if __name__ == '__main__':
    #init_data_csv()
    #parse_temp_to_data(temppath='temp.bak')
    parse_temp_to_data(temppath='temp')


    
