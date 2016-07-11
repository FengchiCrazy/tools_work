#coding:utf-8
from __future__ import unicode_literals
import datetime
from whoosh.query import DateRange, Every
import numpy as np
import pdb

def tfPdf(searcher):
    #pdb.set_trace()
    dayCountLookback = 10
    keywordCount    = 100000
    
    current   = datetime.datetime.now()
    startDay  = current - datetime.timedelta(days=dayCountLookback)
    timeQuery = DateRange("notice_time", startDay, current)
    srcInfo   = searcher.search(timeQuery, groupedby="source")
    #step1: get the documents ids of latest news
    docIDs    = list(srcInfo.docs())
    
    #step2: select some keywords
    #termsInfo = searcher.key_terms(docIDs, "content", keywordCount)
    #terms     = [ix[0] for ix in termsInfo]
    terms = searcher.reader().most_distinctive_terms("content", number=keywordCount)
    terms = [ix[1] for ix in terms]
    
    #step3: calculate the tfpdf scores for each term
    srcDic    = srcInfo.groups()   # {src : [doc id list]}
    indexedInverted = {}
    termWeights     = {}
    for term in terms:
        matcher = searcher.reader().postings("content", term)
        termDic = dict(matcher.items_as("frequency"))
        indexedInverted[term] = termDic
        #calculate Fjc #calculate Njc
        sumChannel = 0.0
        Fjc = np.zeros(len(srcDic))
        Njc = np.zeros(len(srcDic))
        for id, channel in enumerate(srcDic.keys()):
            docs = srcDic[channel]
            Nc   = len(docs)
            njc  = 0
            sumDoc = 0.0
            for doc in docs:
                nc = termDic.get(doc, 0.0)
                sumDoc += nc
                if nc > 0:njc+=1
            sumChannel += sumDoc
            Fjc[id] = sumDoc
            if Nc != 0:
                Njc[id] = np.exp(float(njc) / Nc) 
        #assert sumChannel!=0, "zero frequency of term %s"%term 
        if sumChannel==0:
            termWeights[term] = 0.0
            continue
        Fjc = Fjc / sumChannel
        Fjc = Fjc / np.sqrt(np.sum(np.power(Fjc,2)))
        weight = np.sum(Fjc*Njc)
        termWeights[term] = weight
        print "%s\t%s" %(term.decode('latin-1').encode('utf8'), weight)
        
    #step4: Calculate the document score
    idToDic = {}
    for hit in srcInfo:
        idToDic[hit.docnum] = dict(hit.iteritems())
    docWeights = dict([(x , 0.0) for x in docIDs])
    for doc in docIDs:
        for term in indexedInverted:
            freq = indexedInverted[term].get(doc, 0)
            if freq > 0: #if term occurs in doc
                docWeights[doc] += termWeights[term]  
    docWeights = sorted(docWeights.iteritems(),key=lambda x:x[1],reverse=True)
    res = []
    for line in docWeights:
        docnum = line[0]
        weight = line[1]
        dic    = idToDic[docnum]
        dic['docnum'] = docnum
        dic['score'] = weight
        res.append(dic)
    return res
