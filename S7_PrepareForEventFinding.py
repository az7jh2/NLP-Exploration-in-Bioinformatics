# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 16:40:52 2016

@author: hill103
"""

"""读取数据库中的paper记录，保存成ascii格式文本文件
每一条记录对应一个文本文件，文件名为PubMed ID，即XXXX.txt
文本文件中只有一行，即Abstract
然后将所有文件压缩成tar.gz格式压缩文件，指定每一个压缩包中的文件个数
压缩包的命名规则为inputX.tar.gz
结果：24010篇Abstract，被分至10个tar gz压缩文件中(每一个文件含2500个文本)，用时15s
"""

import peewee as pw
from time import time
import os
from shutil import rmtree  #删除非空文件夹，空文件夹可用os.rmdir删除
import tarfile


#数据库名称定义为全局变量
global DB
DB = pw.SqliteDatabase("Human_Gene_Infos.sqlite")

#含有基因名称的pubmed表格
class PUBMED_GENE_MAPS(pw.Model):
    PUBMED_ID = pw.PrimaryKeyField()
    TITLE = pw.TextField()
    ABSTRACT = pw.TextField()
    GENE_NAME = pw.TextField()
    GENEID = pw.TextField()
    class Meta:
        database = DB
        
#----------------------------------------------------------------------------#
def main(fileNumPerFolder):
    #主函数
    start_time = time()
    cwd = os.getcwd()
    tmpFolderPath = os.path.join(cwd, "Tmp_Files")
    if os.path.exists(tmpFolderPath):
        rmtree(tmpFolderPath)  #如果存在该文件夹，先删除，否则新建文件夹时报错
        print "CAUTION: file folder %s removed!" % tmpFolderPath
    os.mkdir(tmpFolderPath)
    print "temporary file folder %s created!" % tmpFolderPath

    DB.connect()
    fileCount = 0  #记录文本文件的个数
    fileFolderCount = 0  #记录当前文件夹的后缀数字
    for item in PUBMED_GENE_MAPS.select().naive().dicts().iterator():
        fileCount += 1
        if fileCount == 1 or fileCount % fileNumPerFolder == 1:  #需要新建文件夹
            fileFolderCount += 1
            subFolderPath = os.path.join(tmpFolderPath, "input" + str(fileFolderCount))
            os.mkdir(subFolderPath)     
        fileName = os.path.join(subFolderPath, str(item["PUBMED_ID"]) + ".txt")
        with open(fileName, "wt") as f:
            f.write(item["ABSTRACT"].encode("ascii", "ignore"))  #第一行写入Abstract
    DB.close()
    
    """#打包成tar.gz压缩文件，逐个添加文件
    with tarfile.open(os.path.join(cwd, "input.tar.gz",), "w:gz") as tar:
        for root, dirs, files in os.walk(folderPath):
            for txt in files:
                filePath = os.path.join(root, txt)
                #arcname使用文件名，滤除文件夹信息
                tar.add(filePath, arcname = os.path.basename(filePath))"""
    
    #打包整个文件夹，tar.gz压缩包中会出现文件夹目录，使用arcname来滤出根目录信息，只保留最后一层目录
    folderPath = os.path.join(cwd, "Files_For_BioEvent_Extraction")
    if os.path.exists(folderPath):
        rmtree(folderPath)  #如果存在该文件夹，先删除，否则新建文件夹时报错
        print "CAUTION: file folder %s removed!" % folderPath
    os.mkdir(folderPath)
    print "file folder %s created!" % folderPath
    for i in range(1, fileFolderCount + 1):
        with tarfile.open(os.path.join(folderPath, "input" + str(i) + ".tar.gz",), "w:gz") as tar:
            subFolderPath = os.path.join(tmpFolderPath, "input" + str(i))
            tar.add(subFolderPath, arcname=os.path.basename(subFolderPath))
                
    #删除临时文件夹
    rmtree(tmpFolderPath)
    print "temporary file folder %s removed!" % folderPath
    print "total %d files, splitted into %d tar gz parts." % (fileCount, fileFolderCount)
    print "Elapsed time: %.2f seconds." % (time() - start_time)
        
        
if __name__ == "__main__":
    main(2500)