#!/usr/bin/env python
# -*- coding: utf-8 -*
from jigsaw import *
from datetime import datetime
import time

class loader(object):

    def __init__(value):
        value=value

    def etltime(self):
        # 生成etltime
        timestamp=int(round(time.time() * 1000))
        return timestamp

    def merge_node(self,source,query,label,etltime):
        #初始化工具类
        j=jigsaw()
        ##执行
        try:
            #load
            print(datetime.now(),"|同步进度:节点同步开始|",label)
            node=j.extract_node(source,query,label,etltime)
        except Exception as error:
            #抛出异常信息
            print(datetime.now(),"|同步进度:节点同步出现异常|",label)
            print("错误信息:",error)
        else:
            #同步成功
            print(datetime.now(),"|同步进度:节点同步成功|",label)

    def merge_relationship(self,source,query,label,label_end,relationtype):
        #初始化工具类
        j=jigsaw()
        ##执行
        try:
            #load
            print(datetime.now(),"|同步进度:关系同步开始|",label,label_end)
            relationship=j.extract_relationship(source,query,label,label_end,relationtype)
        except Exception as error:
            #抛出异常信息
            print(datetime.now(),"|同步进度:关系同步出现异常|",label,label_end)
            print("错误信息:",error)
        else:
            #同步成功
            print(datetime.now(),"|同步进度:关系同步成功|",label,label_end)

    def create_node(self,source,query,label,etltime):
        #初始化工具类
        j=jigsaw()
        ##执行
        try:
            #load
            print(datetime.now(),"|同步进度:节点导入开始|",label)
            node=j.load_node(source,query,label,etltime)
        except Exception as error:
            #抛出异常信息
            print(datetime.now(),"|同步进度:节点导入出现异常|",label)
            print("错误信息:",error)
        else:
            #同步成功
            print(datetime.now(),"|同步进度:节点导入成功|",label)

    def create_relationship(self,source,query,label,label_end,relationtype):
        #初始化工具类
        j=jigsaw()
        ##执行
        try:
            #load
            print(datetime.now(),"|同步进度:关系导入开始|",label,label_end)
            relationship=j.load_relationship(source,query,label,label_end,relationtype)
        except Exception as error:
            #抛出异常信息
            print(datetime.now(),"|同步进度:关系导入出现异常|",label,label_end)
            print("错误信息:",error)
        else:
            #同步成功
            print(datetime.now(),"|同步进度:关系导入成功|",label,label_end)

    def batch_create_node(self,source,query,label,etltime):
        #初始化工具类
        j=jigsaw()
        ##执行
        try:
            #load
            print(datetime.now(),"|同步进度:节点导入开始|",label)
            node=j.batch_node(source,query,label,etltime)
        except Exception as error:
            #抛出异常信息
            print(datetime.now(),"|同步进度:节点导入出现异常|",label)
            print("错误信息:",error)
        else:
            #同步成功
            print(datetime.now(),"|同步进度:节点导入成功|",label)

    def batch_create_relationship(self,source,query,label,label_end,relationtype):
        #初始化工具类
        j=jigsaw()
        ##执行
        try:
            #load
            print(datetime.now(),"|同步进度:关系导入开始|",label,label_end)
            relationship=j.batch_relationship(source,query,label,label_end,relationtype)
        except Exception as error:
            #抛出异常信息
            print(datetime.now(),"|同步进度:关系导入出现异常|",label,label_end)
            print("错误信息:",error)
        else:
            #同步成功
            print(datetime.now(),"|同步进度:关系导入成功|",label,label_end)

