#coding:utf-8

import sys

lastkey = None
cnt=0

for line in sys.stdin:
  key = line.strip()
  if lastkey and key!=lastkey:
    print "%s\t%s" %(lastkey, cnt) 
    cnt = 0
  lastkey = key
  cnt+=1

print "%s\t%s" %(lastkey,cnt)
