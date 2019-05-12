# -*- coding: utf-8 -*-
"""
Created on Mon Jan 09 12:07:10 2017

@author: hill103
"""

"""归一化基因名称
待归一化的基因名称源自S4 NER的结果，有96917篇Abstract
用于归一化的基因官方(不使用常用非官方名称)源自S5解析的结果，得到基因名称25397(加上非官方共75971)个
归一化的过程采用集合的交集实现，待归一化的基因名称采用空格分割，然后与含有基因官方名称的集合做交集运算
所有名称的字母都转换为大写字母
结果：24010(加上非官方共51831)篇Abstract中找到了官方基因名称，用时13s
"""

import peewee as pw
from time import time
import json


#数据库名称定义为全局变量
global DB, DB2
DB = pw.SqliteDatabase("Human_Gene_Infos.sqlite")
DB2 = pw.SqliteDatabase("pubmed.sqlite")
#定义数据库中的表格模型
class GENE_INFOS(pw.Model):
    #pubmed_id为数字类型，主键
    GENEID = pw.PrimaryKeyField()
    #title和abstract为文本类型
    ENSEMBL = pw.TextField(null = True)  #允许为空，默认参数default=None
    SYMBOL = pw.TextField(null = True)
    SYNONYMS = pw.TextField(null = True)
    #所用数据库为db
    class Meta:
        database = DB  #DB为所定义的数据库变量
#摘要数据库的表格模型
class PUBMED_INFOS(pw.Model):
    #pubmed_id为数字类型，主键
    PUBMED_ID = pw.PrimaryKeyField()
    #title和abstract为文本类型
    TITLE = pw.TextField()
    ABSTRACT = pw.TextField()
    #所用数据库为db
    class Meta:
        database = DB2  #DB为所定义的数据库变量
#新表格
class PUBMED_GENE_MAPS(pw.Model):
    PUBMED_ID = pw.PrimaryKeyField()
    TITLE = pw.TextField()
    ABSTRACT = pw.TextField()
    GENE_NAME = pw.TextField()
    GENEID = pw.TextField()
    class Meta:
        database = DB
        
#------------------------------------------------------------------------------#
def generateGeneName():
    #载入保存gene名称的SQL文件
    DB.connect()
    #提取特定的记录，保存成gene名称为key，gene id为item的dict类型
    gene_name = {}
    """for record in GENE_INFOS.select().where((GENE_INFOS.ENSEMBL.is_null(False)) 
                                          & ((GENE_INFOS.SYMBOL.is_null(False) | 
                                          (GENE_INFOS.SYNONYMS.is_null(False))))).naive().dicts().iterator():"""
    for record in GENE_INFOS.select().where((GENE_INFOS.ENSEMBL.is_null(False))
                                         & (GENE_INFOS.SYMBOL.is_null(False))).naive().dicts().iterator():
        if not record["SYMBOL"] is None:
            symbol = record["SYMBOL"].encode("ascii", "ignore").upper()
            if gene_name.get(symbol) is None:
                gene_name[symbol] = str(record["GENEID"])
            else:
                gene_name[symbol] += ("|" + str(record["GENEID"]))
                
        """if not record["SYNONYMS"] is None:
            synonyms = [s.encode("ascii", "ignore").upper() for s in record["SYNONYMS"].split("|")]
            for s in synonyms:
                if gene_name.get(s) is None:
                    gene_name[s] = str(record["GENEID"])
                else:
                    gene_name[s] += ("|" + str(record["GENEID"]))"""
    DB.close()
    gene_name_set = set(gene_name.keys())
    print "set of %d gene official and unofficial name generated." % len(gene_name_set) 
    return gene_name, gene_name_set

def normGene(gene_name, gene_name_set, ner_result):
    #根据gene名称，解析ner_result的名称是否包含官方名称，从而建立gene和pubmed的对应关系
    #采用set的交集运算来实现gene名称归一化过程
    norm_result = []
    for key, value in ner_result.items():
        #key为PubMed ID，value为需要归一化的名称list
        tmp_dict = {}  #保存归一化后的结果
        gene_to_norm = []  #保存每一个pubmed id包含的单词
        for text in value:
            #遍历所有官方gene名称
            gene_to_norm += [s.encode("ascii", "ignore").upper() for s in text.split()]
        result = set(gene_to_norm) & gene_name_set
        if len(result) > 0:
            tmp_dict["PUBMED_ID"] = int(key)
            for s in result:
                if tmp_dict.get("GENE_NAME") is None:
                    tmp_dict["GENE_NAME"] = s
                else:
                    tmp_dict["GENE_NAME"] += ("|" + s)
                if tmp_dict.get("GENEID") is None:
                    tmp_dict["GENEID"] = gene_name[s]
                else:
                    tmp_dict["GENEID"] += ("|" + gene_name[s])
            norm_result.append(tmp_dict)
    print "after normlization: %d" % len(norm_result)
    return norm_result
 
   
def main():
    start_time = time()
    #载入含有pubmed id和gene的json文件
    with open("NER_Result.json") as f:
        ner_result = json.load(f)
    #生成基因官方名称dict
    (gene_name, gene_name_set) = generateGeneName()
    #比对结果，进行gene归一化
    norm_result = normGene(gene_name, gene_name_set, ner_result)
    #查询pubmed的信息，组合成新的table，并保存
    DB2.connect()
    for d in norm_result:
        #每一个ID查询一次数据库，批量查询时会报too many variables的错误
        query = PUBMED_INFOS.select().where(PUBMED_INFOS.PUBMED_ID == d["PUBMED_ID"]).naive().dicts().get()
        #添加title和Abstract信息
        d["TITLE"] = query["TITLE"]
        d["ABSTRACT"] = query["ABSTRACT"]
    DB2.close()
    #保存所有结果
    DB.connect()
    DB.drop_table(PUBMED_GENE_MAPS, fail_silently = True)
    #重新新建表格
    DB.create_table(PUBMED_GENE_MAPS)
    with DB.atomic():  #这是一种快速保存的方法
        PUBMED_GENE_MAPS.insert_many(norm_result).execute()
    print "Human gene normlization completed and saved to SQL successfully. Elapsed time: %.2f seconds." % (time()-start_time)
    
    
if __name__ == "__main__":
    main()