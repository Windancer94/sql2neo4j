#!/usr/bin/env python
# -*- coding: utf-8 -*
import re
import fire
from loader import *

class worker(object):

    def __init__(value):
        value=value

    def do_node(self,table,source="local"):
        loadtype=re.sub("_.*|.*\.","",table)
        label=re.sub(".*_","",table)
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
        info=re.split(".|_",table)
        loadtype=info[1]
        label=info[2]
        relationtype=info[3]
        label_e=info[4]
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
#    fire.Fire(worker)            # 调用google fire开启命令行
    do_node()
