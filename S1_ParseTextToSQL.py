# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 16:23:48 2017

@author: hill103
"""

"""以utf-8格式打开文件，解析其中的文本，每一行为一篇文章，
格式为"PubMed_ID";"Journal_Name";"Year";"Title";"Abstract";"Page";"Keyword";
其中有些部分可能为"NULL"
将具有Abstract信息的文章存入SQLite数据库，数据库包括对应的3列："PubMed_ID"；"Title";"Abstract"
V3改进：保存Abstract时，去除换行符\n
注意：Ubuntu比Windows，peewee容易报错too many SQL variables，这时候减少一次插入的record数（从10万降到了1万）
结果：共1,776,292篇文献，解析错误0篇，有Abstract的1,004,404篇，用时3min
"""

import peewee as pw
from time import time
import codecs

#数据库名称定义为全局变量
global DB
DB = pw.SqliteDatabase("pubmed.sqlite")
#定义数据库中的表格模型
class PUBMED_INFOS(pw.Model):
    #pubmed_id为数字类型，主键
    PUBMED_ID = pw.PrimaryKeyField()
    #title和abstract为文本类型
    TITLE = pw.TextField()
    ABSTRACT = pw.TextField()
    #所用数据库为db
    class Meta:
        database = DB  #DB为所定义的数据库变量

def newDatabase():
    #新建数据库
    #连接数据库
    DB.connect()
    #检查表格，如果存在该表格，则先删除该表格
    DB.drop_table(PUBMED_INFOS, fail_silently = True)
    #重新新建表格
    DB.create_table(PUBMED_INFOS)
    #关闭数据库
    DB.close()
    
def main(file_name):
    start_time = time()
    newDatabase()  

    failed_count = 0  #解析失败的记录数量
    no_abstract_count = 0  #不含Abstract的记录数量
    record_count = 0  #插入数据库的记录数量
    total_count = 0
    data = []    
    
    #utf-8格式打开文件
    f = codecs.open(file_name, "r", "utf-8")
    #逐行解析，使用readlines会内存超出限制而报错
    for line in f:
        total_count += 1
        line = line.strip()
        #以";"为分隔符
        tmp = line.split('";"')
        if len(tmp) == 7:
            pubmed_id = int(tmp[0][1:])  #去除第一个引号
            title = tmp[3].strip()
            abstract = tmp[4].strip()
        else:
            failed_count += 1
            continue
        
        #将含有abstract的记录转成字典格式，保存list中
        if abstract == u"NULL":
            no_abstract_count += 1
            continue
        else:
            #去除换行符,windows的换行是\r\n，unix的是\n，mac的是\r
            abstract.replace("\n", "")
            abstract.replace("\r", "")
            data.append({"PUBMED_ID":pubmed_id, "TITLE":title, "ABSTRACT":abstract})          
            record_count += 1
        if total_count % 10000 == 0:  #每1万条记录，数据进数据库，并提示一次进度
            with DB.atomic():  #这是一种快速保存的方法
                PUBMED_INFOS.insert_many(data).execute()
            data = []
            print "%d records have been parsed. failed: %d, No abstract: %d, Insert databse: %d." % (total_count, failed_count, no_abstract_count, record_count)
    f.close()
    if len(data) > 0:
        with DB.atomic():  #这是一种快速保存的方法
            PUBMED_INFOS.insert_many(data).execute()
    DB.close()
    print "Total records number: %d." % total_count
    print "Parsing Failed records number: %d." % failed_count
    print "With no abstract records number: %d." % no_abstract_count
    print "Insert into database records number: %d." % record_count
    print "Elapsed time: %.2f seconds." % (time() - start_time)
    
    
if __name__ == "__main__":
    main("Pubmed.csv")