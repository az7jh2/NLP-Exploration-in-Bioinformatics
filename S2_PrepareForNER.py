# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 13:51:05 2017

@author: hill103
"""

"""从数据库中读取记录，转换成txt格式，放在一个文件夹下，用于ABNER进行命名实体识别
从数据库中读出的utf-8格式字符串将会转为ascii格式
为提高性能，每一个文本文档中含有指定数目个Abstract，每一个Abstract占一行
V2改进：V1中对ABNER的格式要求理解错误;
       由于ABNER会自动分为多行，因此在每一篇Abstract的末尾添加标记"#####"，用于后续解析结果时便于区分
       为便于实现并行处理，所有文本文件会分为几个文件夹
结果：共1,004,404篇文献，分成了402个txt文件，5个子文件夹(总数确认正确)，用时15s
"""

from shutil import rmtree  #删除非空文件夹，空文件夹可用os.rmdir删除
from time import time
import peewee as pw
import os

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

#--------------------------------------------------------------------------#
def makeNewFolder(path):
    #新建一个文件夹，path为其完整路径
    if os.path.exists(path):
        rmtree(path)  #如果存在该文件夹，先删除，否则新建文件夹时报错
        print "CAUTION: file folder %s removed!" % path
    os.mkdir(path)
    print "temporary file folder %s created!" % path
    return

def main(num, files):
    #num为每一个文本文档中，包含的Abstract篇数;files为每一个文件夹中包含的文本文档数量
    start_time = time()
    cwd = os.getcwd()
    #新建外层文件夹
    parent_folder_path = os.path.join(cwd, "Files_For_NER")
    makeNewFolder(parent_folder_path)
    
    DB.connect()
    count = 0  #当前处理记录的序号
    file_index = 0  #文本文档的编号
    dir_index = 0  #文件夹的编号
    #查询数据，select表示输出所有列；naive能提高性能；
    #dicts把model转为dict，能提高性能；iterator不会一次缓存全部数据，能减少内存占用
    for item in PUBMED_INFOS.select().naive().dicts().iterator():
        count += 1
        if count == 1 or count % (num*files) == 1:
            #新建文件夹
            dir_index += 1
            sub_folder_path = os.path.join(parent_folder_path, "Files_For_NER"+str(dir_index))
            makeNewFolder(sub_folder_path)
        if count == 1 or count % num == 1:
        #需要新建文本文件
            file_index += 1
            file_full_name = os.path.join(sub_folder_path, "input"+str(file_index)+".txt")
            if count == 1:
                f = open(file_full_name, "wt")
            else:
                f.close()
                f = open(file_full_name, "wt")
        #以pubmed id作为每一行的起始
        #以ASCII格式保存Abstract，每一篇Abstract的末尾添加标记" #####"
        f.write(str(item["PUBMED_ID"])+":"+item["ABSTRACT"].encode("ascii", "ignore")+" #####\n")
    f.close()
    DB.close()
    print "There are %d files in %d folders containing %d records. Elapsed time: %.2f seconds." % (file_index, dir_index, count, time()-start_time)

if __name__ == "__main__":
    main(2500, 100)