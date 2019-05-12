# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 14:55:50 2017

@author: hill103
"""

"""解析ABNER的结果，结果为SGML格式
每一行为一篇Abstract，其中可能存在<XXX> YYY </XXX>的标记
标记共5种：PROTEIN，DNA，RNA，CELL_TYPE,CELL_LINE
每一篇Abstract提取DNA实体，采用集合保存命名实体，以避免重复
采用并行处理，每一个文件夹提交为子任务
结果：共1,004,404篇文献，在402个sgml文件中，其中96917篇找到了DNA实体，用时1min
"""

from bs4 import BeautifulSoup
from time import time
import os, re, json
from multiprocessing import Pool

   
class parse():
    #定义一个parse对象，便于变量追踪
    def __init__(self, path):
        self.path = path
        #结果定义为dict类型，以pubmed id为key，找到的DNA实体的set为item
        self.result = {}
        #用于搜索pubmed id的正则表达式，”数字+：“格式，\A表示仅匹配字符串开头 
        self.pattern = re.compile("\d+ :")
        self.file_count = 0  #记录解析的文件数量
        self.record_count = 0  #记录解析的Abstract数量
        
    def parseOneLine(self, line):
        #解析一篇Abstract，把结果存入result中
        #采用lxml HTML解析器，速度快，容错能力强，如果报错改用Python内置标准库"html.parser"
        soup = BeautifulSoup(line, "lxml")
        if len(soup.find_all("dna")) > 0:  #tag被转换为小写字母
            dna = set()
            for item in soup.find_all("dna"):
                #用text访问解析的结果，为utf8格式，并且头尾都有空格
                dna.add(item.text.strip().encode("ascii", "ignore"))
                #利用正则表达式，获取pubmed id
                #取第一个识别出来的数字，并去除末尾的空格和冒号
                #采用search方法，因为id不一定出现在字符串开头(match只从头开始匹配)
                pubmed_id = int(self.pattern.search(line).group()[:-2])
            #json不支持set格式，转换为list
            self.result[pubmed_id] = list(dna)
        return
    
    def parseFile(self, file_full_name):
        #解析一个文本文件，把结果存入result中
        with open(file_full_name, "rt") as f:
            #读入所有文件，并用自定义标记来分割文本        
            text = f.read()  
            lines = text.split("# # # # #")
            for line in lines:
                line = line.strip()
                if len(line) > 0:
                    #去除最后为空的一行，其余每一行都为一篇Abstract，独立解析
                    self.record_count += 1
                    self.parseOneLine(line)
        return

    def parseFolder(self):
        #解析一个文件夹中的所有文本文件
        #遍历该文件夹中后缀名为sgml的所有文件
        for file_name in os.listdir(self.path):
            if os.path.splitext(file_name)[-1] == ".sgml":  #扩展名为sgml
                self.file_count += 1
                self.parseFile(os.path.join(self.path, file_name))
        return
#----------------------------------------------------------------------------------#

def parseOneFolder(path):
    #解析一个文件夹中的所有文本文件，作为一个子任务并行运行
    p = parse(path)
    print "start parse file folder %s." % path 
    p.parseFolder()
    return p.file_count, p.record_count, p.result
    
def main():
    start_time = time()
    #参数为空，自动调用cpu_count计算CPU核数
    pool = Pool()

    cwd = os.getcwd()
    parent_folder_path = os.path.join(cwd, "Files_For_NER")
    #确定所有子文件夹
    dirlist = []
    for root, sub, file_name in os.walk(parent_folder_path):
        if len(sub) < 1:  #子文件夹数目为0，说明它是最底层子文件夹
            dirlist.append(root)
            
    #每一个子文件夹，对应一个进程，并行处理，并获取结果
    multiple_results = [pool.apply_async(parseOneFolder, (path,)) for path in dirlist]
    #综合各个子任务的结果
    final_result = {}
    file_num = 0  #记录解析的文件数量
    record_num = 0  #记录解析的Abstract数量
    for res in multiple_results:
        tmp = res.get()
        #返回结果为tuple，对最终结果进行更新
        file_num += tmp[0]
        record_num += tmp[1]
        final_result.update(tmp[2])

    #字典类型的结果保存成json格式
    with open("NER_Result.json", "wt") as f:
        json.dump(final_result, f, indent = 4, sort_keys = True)
    print "NER result of %d records in %d files parsing completed." % (record_num, file_num)
    print "There are %d records have DNA entity." % len(final_result.keys())
    print "Elapsed time: %.2f seconds." % (time() - start_time)

if __name__ == "__main__":
    main()