#-*- coding:utf-8 -*-
import ConfigParser
import sys
import os
from select_mysql import SelectMySQL
from Indices_derived import EvaluateIndex
from collections import OrderedDict
import MySQLdb
import datetime
import pandas as pd
import pdb

COMMA = ','


class GetIndexes(SelectMySQL):
    def __init__(self, **kwargs):
        super(GetIndexes, self).__init__()
        self.config_file_name = kwargs.pop('config_file_name', 'conf.ini')
        self.date             = kwargs.pop('date', datetime.datetime.now().strftime('%Y-%m-%d'))

        self.fund_list = []
        self.PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'data'))
        try:
            self._load_conf(self.config_file_name)
        except ConfigParser.NoSectionError as e:
            sys.stderr.write('ALL SECTION NAMES are not allowed to change!\n')
            sys.stderr.write('You may changed those SECTION NAMES in the config file.\n')
            sys.stderr.write('Please refer to the DEFAULT config file and follow the guidelines!\n')
            sys.stderr.write('ERROR INFO: {}\n'.format(e))
            sys.exit()
        except ConfigParser.NoOptionError as e:
            sys.stderr.write('Some PROTECTED OPTION NAMES are not allowed to change!\n')
            sys.stderr.write('You may changed those PROTECTED OPTION NAMES in the config file.\n')
            sys.stderr.write('Please refer to the DEFAULT config file and follow the guidelines!\n')
            sys.stderr.write('ERROR INFO: {}\n'.format(e))
            sys.exit()

    def _load_conf(self, config_file_name):
        def config_get(classname, optionname):
            ret = config.get(classname, optionname)
            ret = ret.strip().split(COMMA)
            return [x.strip() for x in ret if x != '']

        config = ConfigParser.ConfigParser()
        config.read(config_file_name)

        self.categories = config_get('general', 'category') 
        self.set_funds = config_get('general', 'security')
        self.compare_between_time = config.get('general', 'compare_between_time')
        if self.compare_between_time.strip().upper() == 'TRUE':
            self.compare_between_time = True
        elif self.compare_between_time.strip().upper() == 'FALSE':
            self.compare_between_time = False
        else:
            raise ValueError('compare_between_time in conf.ini must be set True or False')

        self.evaluateIndexes = config.items('evaluate indexes')
        self.evaluateIndexes = [ei[0] for ei in self.evaluateIndexes]

        self.time = config.items('time')
        self.intervals = OrderedDict() 
        for t in self.time:
            self.intervals[t[0]] = [x.strip() for x in t[1].strip().split(COMMA) if x != '']
        
        self.per_nav_list = config_get('method', 'per_nav') 
        self.wan_per_nav_list = config_get('method', 'wan_per_nav')
        

    def lagDate(self, date, interval, lagTime):
        """
        Count the lag date from from 'date'. If lagTime is not integer, the lag date will return None.
        
        """
        try:
            lagTime = int(lagTime)
        except:
            return None

        if interval == 'day':
            timedelta = datetime.timedelta(days=lagTime)
            date = datetime.datetime.strptime(date, "%Y-%m-%d")
            new_date = (date - timedelta).strftime("%Y-%m-%d")

        elif interval == 'month':
            if lagTime == 0:
                new_date = date[0:8] + '01'
            elif lagTime > 0:
                year = int(date[0:4])
                month = int(date[5:7])
                delta_year = 0
                if lagTime > 12:
                    delta_year = lagTime / 12

                month_new = month - lagTime

                while month_new <= 0:
                    month_new += 12

                year = year - delta_year
                month_new = str(month_new)
                if len(month_new) < 2:
                    month_new = '0' + month_new
                new_date = str(year) + '-' + month_new + date[7:10]

            else:
                raise ValueError('LookBack month must be great than 0')

        elif interval == 'year':
            if lagTime == 0:
                new_date = date[0:4] + '-01-01'
            elif lagTime > 0:
                new_date = str(int(date[0:4]) - lagTime) + date[4:]
            else:
                raise ValueError('Lookback year must be great than 0')

        return new_date

    def getEIResults(self, code, category, interval, lagTime):
        lag_date = self.lagDate(self.date, interval, lagTime)
        series = self.getList(code, lag_date, self.date)
        ret = []
        if series is not None:
            ei = self.buildEvaluateIndexesObject(category, series) 
            for index in self.evaluateIndexes:
                func = getattr(ei, index) 
                res = func()
                if res is not None:
                    ret.append("%.08f"%res)
                else:
                    ret.append('')
        else:
            ret = ['' for x in self.evaluateIndexes]

        return ret

    def outputEverySingleCsv(self, category, interval, lagTime):
        dirpath = "%s%s%s_%s_%s.csv" % (self.PATH, os.sep, category, interval, lagTime)
        columns = ['company'] + self.evaluateIndexes
        arr = []
        index = []
        for code in self.fund_list:
            res = self.getEIResults(code, category, interval, lagTime)
            name = self.getCompanyName(code)
            index.append(code)
            arr.append([name] + res)
        df = pd.DataFrame(arr , index=index , columns = columns)
        df.to_csv(dirpath)

    def chooseFunction(self, category):
        if category in self.per_nav_list:
            return self.getPerNavList 
        elif category in self.wan_per_nav_list:
            return self.getReturnSeriesList
        else:
            raise ValueError("not defined proper function for category" + category)
    
    def buildEvaluateIndexesObject(self, category, series):
        if category in self.per_nav_list:
            return EvaluateIndex(series,is_price_series=True) 
        elif category in self.wan_per_nav_list:
            return EvaluateIndex(series,is_price_series=False)
        else:
            raise ValueError("not defined proper function for category" + category)

    def getListOfYieldRate(self):
        for category in self.categories:
            if len(self.set_funds) > 0:
                self.fund_list = self.set_funds
            else:
                self.fund_list = self.getListFundOfDay(category)

            self.getList = self.chooseFunction(category)

            if self.compare_between_time == False:
                for interval, lagTimes in self.intervals.items():
                    for lt in lagTimes:
                 
                        self.outputEverySingleCsv(category, interval, lt)
            else:
                self.outputCompareTimeCsv(category)
                   
    def outputCompareTimeCsv(self, category):
        dirpath = "%s%s%s.csv" % (self.PATH, os.sep, category)
        columns = ['company']
        for index in self.evaluateIndexes:
            for interval, lagTimes in self.intervals.items():
                for lt in lagTimes:
                    columns.append("%s_%s_%s" % (index, interval, lt))
        df = pd.DataFrame( index = self.fund_list, columns = columns)

        for interval, lagTimes in self.intervals.items():
            for lt in lagTimes:
                for code in self.fund_list:
                    res = self.getEIResults(code, category, interval, lt)
                    for i in range(len(res)):
                        idx = self.evaluateIndexes[i]
                        column_idx = "%s_%s_%s" % (idx, interval, lt)
                        df.loc[code, column_idx] = res[i]
        
        for code in self.fund_list:
            name = self.getCompanyName(code)
            df.loc[code, 'company'] = name

        df.to_csv(dirpath)

        #pdb.set_trace()

if __name__ == '__main__':
    gi = GetIndexes(date = '2016-05-10')
    
    gi.getListOfYieldRate()

    

