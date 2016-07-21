#coding:utf-8
import sys, os
import MySQLdb
import logging

class dataBase(object):

    def __init__(self):
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
        self.conn = conn
        self.cursor = cursor
        dirpath        = os.path.split(os.path.realpath(__file__))[0]
        self.dirpath   = os.path.dirname(dirpath)

    def _getLogger(self, filename):
        level = logging.DEBUG
        file_path = self.dirpath + os.sep + 'log' + os.sep + filename
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')

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
 
