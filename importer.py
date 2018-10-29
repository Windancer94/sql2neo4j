#!/usr/bin/env python
# -*- coding: utf-8 -*
import re
import fire
from import_helper import *


class worker(object):

    def __init__(value):
        value=value

    def do_node(self,table,source="local"):
        info=[]
        info=re.split("_",re.sub(".*\.","",table))
        print(info)
        loadtype=info[0]
        label=info[1]
        node=f'''select * from {table}'''
        l=loader()
        etltime=l.etltime()
        if loadtype=='''create''':
            l.create_node(source,node,label,etltime)
        elif loadtype=='''merge''':
            l.merge_node(source,node,label,etltime)
        elif loadtype=='''batch_create''':
            l.batch_create_node(source,node,label,etltime)
        else:
            return '''loadtype不存在'''

    def do_relationship(self,table,source="local"):
        info=[]
        info=re.split("_",re.sub(".*\.","",table))
        print(info)
        loadtype=info[0]
        label=info[1]
        relationtype=info[2]
        label_e=info[3]
        relationship=f'''select * from {table}'''
        l=loader()
        etltime=l.etltime()
        if loadtype=='''create''':
            l.create_relationship(source,relationship,label,label_e,relationtype)
        elif loadtype=='''merge''':
            l.merge_relationship(source,relationship,label,label_e,relationtype)
        elif loadtype=='''batch_create''':
            l.batch_create_relationship(source,relationship,label,label_e,relationtype)
        else:
            return '''loadtype不存在'''

if __name__=='''__main__''':
    fire.Fire(worker)            # 调用google fire开启命令行
    # 使用举例
    # w=worker()
    # w.do_node("s_demo.create_class")
    # w.do_node("s_demo.create_dept")
    # w.do_relationship("s_demo.create_class_abstract_dept")
