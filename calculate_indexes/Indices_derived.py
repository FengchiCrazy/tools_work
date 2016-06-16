#coding:utf-8

import pdb
import numpy as np
from decimal import Decimal

class EvaluateIndex(object):
    '''
    brief:
    
    input: daily price series
           return risk free
    output:
           1,daily return series  :  get_return_series()
           2,annual profit ratio  :  annual_return()
           3,standard deviation   :  standard_deviation()
           4,sharp rati           :  sharp_ratio()
           5,max drawdown         :  max_drawdown() 
    '''
    
    def __init__(self, series, is_price_series=True):
        """
        series can be pirce series or return series. If price series, the second param
        will be true, and if return series, the sencond param will be false.
        """
        self.__isDecimal = isinstance(series[0] , Decimal)
        if is_price_series:
            self.price_series = series
            self.return_series = self.__get_return_series()
        else:
            self.return_series = series
            self.price_series = self.__get_price_series()
        

    def __get_return_series(self):
        a = self.price_series
        tmp = 0.0
        if self.__isDecimal: 
            tmp = Decimal(tmp)
        return_series =  [tmp]
        for i in range(1, len(a)):
            return_series.append((a[i] - a[i-1]) / a[i-1])
        return return_series

    def __get_price_series(self):
        #pdb.set_trace()
        a = self.return_series
        start_price = 1.0
        if self.__isDecimal: 
            tmp = Decimal(start_price) * (1 + a[0])
            price_series = [Decimal(0.0) for x in a]
        else:
            tmp = start_price * (1 + a[0])
            price_series = [0.0 for x in a]
        price_series[0] = tmp
        for i in range(1, len(a)):
            price_series[i] = price_series[i - 1] * (1 + a[i])
        return price_series


    def annual_return(self):
        days = len(self.price_series)
        p = (days+1) / 252.0
        price_final = self.price_series[days-1]
        price_first = self.price_series[0]
        tmp1 = 1.0/p
        tmp2 = 1.0
        if self.__isDecimal:
          tmp1 = Decimal(tmp1)
          tmp2 = Decimal(tmp2)
        r = pow(price_final/price_first,tmp1) - tmp2
        return r


    def annual_volatility(self):
        daily_volatility = np.std(self.return_series)
        tmp = np.sqrt(252.0)
        if self.__isDecimal:tmp = Decimal(tmp)
        annual_volatility = daily_volatility * tmp
        return annual_volatility

    def sharp_ratio(self , rf = 0.0):
        try:
            tmp = np.sqrt(252.0)
            if self.__isDecimal:
                rf = Decimal(rf)
                tmp = Decimal(tmp)
            excess_ret = np.array(self.return_series) - rf
            sharp_ratio = np.mean(excess_ret) / np.std(excess_ret)

            return sharp_ratio * tmp 
        except:
            return None

    def max_drawdown(self):
        a = self.price_series
        drawdown = [0.0 for x in range(0, len(self.price_series))]
        M = drawdown[0]
        for i in range(1, len(a)):
            tmp = float(a[i])
            if tmp > M:
                M = tmp
            if M == tmp:
               drawdown[i] = 0.0
            else:
               drawdown[i] = (M - tmp + 0.0) / M
        max_drawdown = max(drawdown)
        if self.__isDecimal:
            max_drawdown = Decimal(max_drawdown)
        return max_drawdown

  
    def calmar_ratio(self , rf=0.03):
        if self.__isDecimal:rf = Decimal(rf)
        excess_ret = self.annual_return() - rf
        if np.fabs(float(self.max_drawdown() )) >1e-6:
            calmar_ratio = excess_ret / self.max_drawdown()
        else:
            calmar_ratio = None
        return calmar_ratio
   
if __name__ == '__main__':
    ei = EvaluateIndex([4, 5, 3, 2, 0.56])
    print ei.annual_return()
    print ei.annual_volatility()
    print ei.sharp_ratio()
    print ei.max_drawdown()
    print ei.calmar_ratio()
