#!/bin/bash

#step1, load data
echo "python newsIndex.py"
python newsIndex.py
#step2, hot rank
echo "python hotNews.py"
python hotNews.py
#step3, relRank
echo "python relRank.py"
python relRank.py
