# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 16:03:38 2017

@author: hill103
"""

"""查询结果
"""

import peewee as pw
from time import time

#数据库名称定义为全局变量
global DB
DB = pw.SqliteDatabase("Human_Gene_Infos.sqlite")

#含有sentence信息的表格
class SENTENCE_INFOS(pw.Model):
    SENTENCE = pw.TextField()
    PUBMED_ID = pw.IntegerField()
    ENTITY = pw.TextField()
    BIOEVENT = pw.TextField()
    class Meta:
        database = DB

#含有pubmed信息的表格
class PUBMED_GENE_MAPS(pw.Model):
    PUBMED_ID = pw.PrimaryKeyField()
    TITLE = pw.TextField()
    ABSTRACT = pw.TextField()
    GENE_NAME = pw.TextField()
    GENEID = pw.TextField()
    class Meta:
        database = DB

#-----------------------------------------------------------------------------#
def outputOneResult(pubmed_id, result):
    #输出一个dict类型的查询结果，打印至屏幕上
    print "Gene name: %s" % result["gene"]
    print "PubMed ID: %d" % pubmed_id
    print "Bioevents: %s" % ",".join(result["bioevent"])
    print "Title: %s" % result["title"]
    #print "Abstract: %s" % result["abstract"]
    for text in result["sentence"]:
        print "Sentence: %s" % text
    print "-" * 50
    
def main(gene, bioevents):
    #在数据库中查询gene和bioevent的记录，显示在屏幕上
    #gene是基因的名称，而bioevent是一个list
    start_time = time()
    DB.connect()
    result = {}
    for record in SENTENCE_INFOS.select().where((SENTENCE_INFOS.ENTITY == gene)
                                         & (SENTENCE_INFOS.BIOEVENT << bioevents)).naive().dicts().iterator():
        #按pubmed id整理结果
        if result.get(record["PUBMED_ID"]) is None:
            #新建一个嵌套dict
            result[record["PUBMED_ID"]] = {}
            result[record["PUBMED_ID"]]["sentence"] = [record["SENTENCE"]]
            result[record["PUBMED_ID"]]["bioevent"] = set([record["BIOEVENT"]])
            result[record["PUBMED_ID"]]["gene"] = record["ENTITY"]
            #查询该id对应的
            tmp = PUBMED_GENE_MAPS.select().where(PUBMED_GENE_MAPS.PUBMED_ID == record["PUBMED_ID"]).dicts().get()
            result[record["PUBMED_ID"]]["title"] = tmp["TITLE"]
            result[record["PUBMED_ID"]]["abstract"] = tmp["ABSTRACT"]
        else:
            #添加sentence记录
            result[record["PUBMED_ID"]]["sentence"].append(record["SENTENCE"])
            result[record["PUBMED_ID"]]["bioevent"].add(record["BIOEVENT"])
    
    DB.close()
    #统计sentence数量
    count = 0
    for key, value in result.items():
        outputOneResult(key, value)
        count += len(value["sentence"])
        
    print "total articles find: %d, sentence find: %d" % (len(result), count)
    print "Elapsed time: %.2f seconds." % (time() - start_time)

if __name__ == "__main__":
    main("TP53", ["Gene_expression", "Regulation"])