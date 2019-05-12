# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 19:37:35 2016

@author: hill103
"""

"""解析TEES的结果，结果为XXXX-pred.xml.gz压缩文件，里面是一个xml格式文件
多个TEES结果文件在同一文件夹下，并行处理，结果保存至SQL数据库中
在结果解析的同时，进行基因名称的归一化，以及regualtion中的positive和negative合为一个
结果：在7703篇文献中找到了14452个Sentence，覆盖2239个gene，用时3min
"""

import xml.etree.ElementTree as ET
import peewee as pw
from time import time
import os, gzip
from multiprocessing import Pool


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

def newTable():
    #新建表格
    #连接数据库
    DB.connect()
    #检查表格，如果存在该表格，则先删除该表格
    DB.drop_table(SENTENCE_INFOS, fail_silently = True)
    #重新新建表格
    DB.create_table(SENTENCE_INFOS)
    #关闭数据库
    DB.close()

#------------------------------------------------------------------------------#
def generateGeneName():
    #载入保存gene名称的SQL文件
    DB.connect()
    #提取特定的记录，保存成gene名称为key，gene id为item的dict类型
    gene_name = set()
    for record in GENE_INFOS.select().where((GENE_INFOS.ENSEMBL.is_null(False))
                                         & (GENE_INFOS.SYMBOL.is_null(False))).naive().dicts().iterator():
        gene_name.add(record["SYMBOL"].encode("ascii", "ignore").upper())
    DB.close()
    print "set of %d gene official name generated." % len(gene_name) 
    return gene_name
    
def parseXML(gene_name, file_path):
    #解析单个XML文件
    result = []
    with gzip.open(file_path, "rb") as f:
        xml = f.read()
        print "gz file %s has been uncompressed." % file_path
        root = ET.fromstring(xml)  #解析xml文件
        print "xml file tree %s has been constructed." % file_path
        for document in root:  #最外层是document标签
            pubmed_id = document.attrib["origId"]
            for sentence in document:  #下一层是sentence标签
                #记录sentence的文本信息
                text = sentence.attrib["text"]
                if sentence.find("interaction") == None:  #寻找该sentence中是否存在event
                    continue
                #发现event，需要记录所有entity，以备查询使用
                entities = {}
                for entity in sentence.findall("entity"):
                    #嵌套dict
                    entities[entity.attrib["id"]] = {"text": entity.attrib["text"], "type": entity.attrib["type"]}
                #记录event，以及对应的entity
                #为避免重复，建立一个set
                entity_envent = set()
                for interaction in sentence.findall("interaction"):
                    e2 = interaction.attrib["e2"]  #entity
                    e1 = interaction.attrib["e1"]  #event
                    #进行gene名称归一化
                    gene_to_norm = [s.encode("ascii", "ignore").upper() for s in entities[e2]["text"].split()]
                    gene_after_norm = set(gene_to_norm) & gene_name
                    if len(gene_after_norm) > 0:
                        #整理bioevent，regualtion中的positive和negative合为一个
                        if entities[e1]["type"] == "Negative_regulation" or entities[e1]["type"] == "Positive_regulation":
                            bioevent = "Regulation"
                        else:
                            bioevent = entities[e1]["type"]
                        #向set中插入一个tuple
                        for name in gene_after_norm:
                            entity_envent.add((name, bioevent))
                #统计该sentence的结果，以dict的形式添加至list中
                if len(entity_envent) > 0:
                    for item in entity_envent:
                        result.append({"SENTENCE":text, "PUBMED_ID":int(pubmed_id), "ENTITY":item[0],
                                   "BIOEVENT":item[1]})
    return result

#--------------------------------------------------------------------------------------------------------#
def main():
    start_time = time()
    #参数为空，自动调用cpu_count计算CPU核数
    pool = Pool()

    cwd = os.getcwd()
    folder_path = os.path.join(cwd, "TEES_Result")
    #确定文件夹下所有文件
    filelist = []
    for file_name in os.listdir(folder_path):
        filelist.append(os.path.join(folder_path, file_name))
    
    #生成用于归一化的gene名称
    gene_name = generateGeneName()
    #多线程解析XML文件
    multiple_results = [pool.apply_async(parseXML, (gene_name, file_path,)) for file_path in filelist]
    #综合各个子任务的结果
    final_result = []
    for res in multiple_results:
        final_result += res.get()
    
    #保存至数据库
    newTable()
    with DB.atomic():  #这是一种快速保存的方法
        SENTENCE_INFOS.insert_many(final_result).execute()
    #统计基因名称
    names_for_count = set()
    for d in final_result:
        names_for_count.add(d["ENTITY"])
    print "result parsing completed and %d sentences coverd %d genes saved into SQL." %  (len(final_result), len(names_for_count))
    print "Elapsed time: %.2f seconds." % (time() - start_time)     
  
      
if __name__ == "__main__":
    main()