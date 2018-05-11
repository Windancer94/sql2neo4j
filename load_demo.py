#!/usr/bin/env python
# -*- coding: utf-8 -*
from import_helper import loader

##参数
#数据源类型
source='local'

#节点
#res_subclass节点
n_res_subclass='''select * from test.n_res_subclass'''
#res_class节点
n_res_class='''select * from test.n_res_class'''
#res_dept节点
n_res_dept='''select * from test.n_res_dept'''
#res_div节点
n_res_div='''select * from test.n_res_div'''
#节点标签
l_res_subclass='''res_subclass:res_search'''
l_res_class='''res_class:res_search'''
l_res_dept='''res_dept:res_search'''
l_res_div='''res_div:res_search'''
l_res='''res:res_search'''
#关系
#dept_div关系
r_dept_div='''select * from test.r_dept_div'''
#class_dept关系
r_class_dept='''select * from test.r_class_dept'''
#subclass_class关系
r_subclass_class='''select * from test.r_subclass_class'''
#res_subclass关系
r_res_subclass='''select * from test.r_res_subclass'''
#关系类型
l_abstract='''abstract {name:"abstract"}'''
#main方法执行
def main():
    l=loader()
    #生成参数
    #etl批次时间戳
    etltime=l.etltime()
    #执行
    l.create_node(source,n_res_subclass,l_res_subclass,etltime)
    l.create_node(source,n_res_class,l_res_class,etltime)
    l.create_node(source,n_res_dept,l_res_dept,etltime)
    l.create_node(source,n_res_div,l_res_div,etltime)
    l.create_relationship(source,r_dept_div,l_res_dept,l_res_div,l_abstract)
    l.create_relationship(source,r_class_dept,l_res_class,l_res_dept,l_abstract)
    l.create_relationship(source,r_subclass_class,l_res_subclass,l_res_class,l_abstract)
    l.create_relationship(source,r_res_subclass,l_res,l_res_subclass,l_abstract)
if __name__=='''__main__''':
    main()
