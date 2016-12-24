#! /usr/bin/env python
# -*- coding: utf-8 -*-

# *************************************************************
#       Filename @  dta_to_csv.py
#         Author @  Fengchi
#    Create date @  2016-12-22 20:02:35
#  Last Modified @  2016-12-24 15:54:28
#    Description @  transform dta file to csv
# *************************************************************

import os
import pandas as pd
import numpy as np

for file_name in os.listdir():
    if file_name[-3:] == 'dta':
        file_name_core = file_name.split('.')[0]
        data = pd.io.stata.read_stata(file_name)
        data.to_csv('%s.csv' % file_name_core)
        
        print(file_name, 'finished!')
