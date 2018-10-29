#!/usr/bin/env python
# -*- coding: utf-8 -*
import re
import fire
from import_helper import *


class worker(object):

    def __init__(value):
        value=value

    def do_node(self,table,source="local",loadtype='auto'):
        info=[]
        info=re.split("_",re.sub(".*\.","",table))
        if loadtype=='auto':
            loadtype=info[0]
        else:
            pass
        label=info[1]
        node=f'''select * from {table}'''
        print('导入方式:',loadtype)
        l=loader()
        etltime=l.etltime()
        if loadtype=='''create''':
            l.create_node(source,node,label,etltime)
        elif loadtype=='''merge''':
            l.merge_node(source,node,label,etltime)
        elif loadtype=='''batch''':
            l.batch_create_node(source,node,label,etltime)
        else:
            return '''loadtype不存在'''

    def do_relationship(self,table,source="local",loadtype='auto'):
        info=[]
        info=re.split("_",re.sub(".*\.","",table))
        if loadtype=='auto':
            loadtype=info[0]
        else:
            pass
        label=info[1]
        relationtype=info[2]
        label_e=info[3]
        relationship=f'''select * from {table}'''
        print('导入方式:',loadtype)
        l=loader()
        etltime=l.etltime()
        if loadtype=='''create''':
            l.create_relationship(source,relationship,label,label_e,relationtype)
        elif loadtype=='''merge''':
            l.merge_relationship(source,relationship,label,label_e,relationtype)
        elif loadtype=='''batch''':
            l.batch_create_relationship(source,relationship,label,label_e,relationtype)
        else:
            return '''loadtype不存在'''

    def auto_import(self,source,node_tables,relationship_tables,loadtype='auto'):
        '''
        自动导入

        '''
        w = worker()
        print('任务开始:',datetime.now())
        for node_table in node_tables:
            w.do_node(node_table,source,loadtype)
        for relationship_table in relationship_tables:
            w.do_relationship(relationship_table,source,loadtype)
        print('任务结束:',datetime.now())


def main():
    pass

if __name__=='''__main__''':
    main()
