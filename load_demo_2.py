#!/usr/bin/env python
# -*- coding: utf-8 -*
from import_helper import loader
from importer import *


def data_import(source):
    #节点
    n_content='''select * from test.merge_content'''
    n_term='''select * from test.merge_term'''
    n_entity='''select * from test.merge_entity'''
    #节点标签
    l_content='''content'''
    l_term='''term'''
    l_entity='''entity'''
    #关系
    r_content_term='''select * from test.merge_content_watching_term'''
    r_term_entity='''select * from test.merge_term_watching_entity'''
    r_entity_entity='''select * from test.merge_entity_watching_entity'''
    #关系类型
    l_watching='''watching {name:"watching"}'''
    l=loader()
    #生成参数
    #etl批次时间戳
    etltime=l.etltime()
    #执行
    l.create_node(source,n_content,l_content,etltime)
    l.create_node(source,n_term,l_term,etltime)
    l.create_node(source,n_entity,l_entity,etltime)
    l.create_relationship(source,r_content_term,l_content,l_term,l_watching)
    l.create_relationship(source,r_term_entity,l_term,l_entity,l_watching)
    l.create_relationship(source,r_entity_entity,l_entity,l_entity,l_watching)

def auto_import(source):
    '''
    自动导入

    '''
    node_tables = ['''test.merge_content''','''test.merge_term''','''test.merge_entity''']
    relation_tables = ['''test.merge_content_watching_term''','''test.merge_term_watching_entity''','''test.merge_entity_watching_entity''']
    w = worker()
    print('导入开始',datetime.now())
    for node_table in node_tables:
        w.do_node(node_table,source)
        print('节点导入完成',datetime.now())
    for relation_table in relation_tables:
        w.do_relationship(relation_table,source)
        print('关系导入完成',datetime.now())

#main方法执行
def main():
    source='jdbc:mysql://127.0.0.1:3306/?user=...&password=...'
    auto_import(source)

if __name__=='''__main__''':
    main()
