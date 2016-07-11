#!/bin/bash

#step1, load data
echo "/data11/home/yangyang.tang/anaconda/bin/python newsIndex.py"
/data11/home/yangyang.tang/anaconda/bin/python newsIndex.py
#step2, hot rank
echo "/data11/home/yangyang.tang/anaconda/bin/python hotNews.py"
/data11/home/yangyang.tang/anaconda/bin/python hotNews.py
#step3, relRank
echo "/data11/home/yangyang.tang/anaconda/bin/python relRank.py"
/data11/home/yangyang.tang/anaconda/bin/python relRank.py
