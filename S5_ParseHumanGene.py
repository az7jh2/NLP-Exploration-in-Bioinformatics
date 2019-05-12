# -*- coding: utf-8 -*-
"""
Created on Sun Jan 08 11:27:29 2017

@author: hill103
"""

"""解析人类基因文件，为基因名称标准化作准备
地址：ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
格式：tab分隔，每一行为一个基因（GeneID标识），第一行为Column header
所需Column：GeneID、Symbol（相当于官方名称）、Synonyms（相当于非官方名称，用|分隔）
包含三个种族：9606（Homo spaniens），63221（Neanderthal尼安德特人）,741158（Denisova索瓦人）
只考虑Homo spaniens，每一行最后为该gene的更新时间

地址：ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2ensembl.gz
格式：tab分隔，每一行为一个基因（GeneID标识）的match，第一行为Column header
所需Column：tax_id（物种编号），GeneID、Ensembl_gene_identifier
每一行同一个基因可能存在多个转录情况
match的信息在ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/README_ensembl中有说明
本次使用的match信息为Homo sapiens Annotation Release 108和GRCh38.p7，时间2016-12-10

此次归一化，只考虑具有ENSG编号的基因，记录其名称，把结果存入数据库
结果：选择种族9606，NCBI中含有GeneID记录59597，具有Ensembl编号的只有25397条记录
其中存在一个GeneID对应多个Ensembl编号的情况
验证：执行SQL语句：select count(*) from gene_infos where ENSEMBl is not Null，返回25397，结果一致
用时12s
"""

import gzip, csv
import peewee as pw
from time import time

#数据库名称定义为全局变量
global DB
DB = pw.SqliteDatabase("Human_Gene_Infos.sqlite")
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

def newDatabase():
    #新建数据库
    #连接数据库
    DB.connect()
    #检查表格，如果存在该表格，则先删除该表格
    DB.drop_table(GENE_INFOS, fail_silently = True)
    #重新新建表格
    DB.create_table(GENE_INFOS)
    #关闭数据库
    DB.close()
    
#------------------------------------------------------------------------------#
def checkFilter(data, filter_fields):
    #检查该记录是否符合条件
    flag = True
    if len(filter_fields) < 1:
        return flag
    else:
        for condition in filter_fields:
            #解析条件，为"XX = YY"格式
            tmp = condition.split("=")
            key = tmp[0].strip()
            item = tmp[1].strip()
            if data[key] != item:
                #不满足filter条件
                flag = False
                break
    return flag
            
def getInfoFromFile(file_name, needed_fields, filter_fields):
    #采用gzip打开gz压缩文件，采用csv模块读取其中的文本
    #needed_fields为所需的field, filter_fields为挑选field满足该条件的record
    with gzip.open(file_name) as f:
        reader = csv.DictReader(f, delimiter = "\t")  #不提供header信息，则认为第一行为header
        field_name = reader.fieldnames
        #判断所需的fields是否有效
        for item in needed_fields:
            if item not in field_name:
                needed_fields.remove(item)
                print "Causion: field name %s is not valid and removed!" % item
        #检查filter中的fields是否有效
        if len(filter_fields) > 0:
            for condition in filter_fields:
                tmp = condition.split("=")
                key = tmp[0].strip()
                item = tmp[1].strip()
                if key not in field_name:
                    filter_fields.remove(condition)
                    print "Causion: filter condition %s is not valid and removed!" % condition
        #输出为一个list，每一个元素为字典
        result = []
        n = 0
        for row in reader:
            if checkFilter(row, filter_fields):
                tmp_result = {}
                for item in needed_fields:
                    tmp_result[item] = row[item]
                result.append(tmp_result)
                n += 1
        print "file %s parsing completed. records: %d." % (file_name, n) 
        return result

    
def main(gene_file, ensembl_file):
    start_time = time()
    newDatabase()
    
    #从gene_file中提取所需信息
    gene_info = getInfoFromFile(gene_file, ["#tax_id","GeneID","Symbol","Synonyms"], ["#tax_id=9606"])
    #从ensembl_file中提取所需信息
    ensembl_info = getInfoFromFile(ensembl_file, ["#tax_id","GeneID","Ensembl_gene_identifier"], ["#tax_id=9606"])
    #因为set不支持dict类型，所以需手动去除重复信息，并建立一个以GeneID为key的dict
    gene_to_ensembl = {}
    for d in ensembl_info:
        if d["#tax_id"] != "9606":
            raise Exception("tax id error!")
        #如果是一个新GeneID，则新建一个set
        if gene_to_ensembl.get(d["GeneID"]) is None:
            gene_to_ensembl[d["GeneID"]] = set()
        gene_to_ensembl[d["GeneID"]].add(d["Ensembl_gene_identifier"])
    #统计1个GeneID对应多个Ensembl编号的记录，多个记录合成一个字符串，用"|"分割
    for key, value in gene_to_ensembl.items():
        if len(value) > 1:
            gene_to_ensembl[key] = "|".join(list(value))
        elif len(value) == 1:
            gene_to_ensembl[key] = list(value)[0]
        else:
            raise Exception("empty value in dictionary!")
    print "mapping gene to ensembl completed. There are %d unique records." % len(gene_to_ensembl.keys())
    #合并信息
    data_to_sql = []
    for d in gene_info:
        if d["#tax_id"] == "9606":
            tmp_dict = {}
        else:
            raise Exception("tax id error!")
        #复制信息，key都为大写字母
        tmp_dict["GENEID"] = int(d["GeneID"])
        if d["Symbol"] != "-":
            tmp_dict["SYMBOL"] = d["Symbol"]
        if d["Synonyms"] != "-":
            tmp_dict["SYNONYMS"] = d["Synonyms"]
        #添加ensembl信息，如果没有，则不添加
        if not gene_to_ensembl.get(d["GeneID"]) is None:
            tmp_dict["ENSEMBL"] = gene_to_ensembl[d["GeneID"]]
        data_to_sql.append(tmp_dict)
    
    with DB.atomic():  #这是一种快速保存的方法
        GENE_INFOS.insert_many(data_to_sql).execute()
    print "Human gene information saved to SQL successfully. Elapsed time: %.2f seconds." % (time()-start_time)
    
    
if __name__ == "__main__":
    main(r"Homo_sapiens.gene_info.gz", r"gene2ensembl.gz")