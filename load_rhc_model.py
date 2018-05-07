#!/usr/bin/env python
# -*- coding: utf-8 -*
from import_helper import loader

##参数
#数据源类型
source='''local'''

#节点
#user节点
n_user='''select * from test.rhc_user;'''
#res节点
n_res='''select * from test.rhc_res;'''
#scene节点
n_scene='''select * from test.rhc_scene;'''
#节点标签
l_user='''user'''
l_res='''res:res_search'''
l_scene='''scene'''
#关系
#user_res关系
r_user_res='''select * from test.rhc_user_res where pay_complete_date>=20180201 and pay_complete_date<20180301;'''
#user_scene关系
r_user_scene='''select * from test.rhc_user_scene where pay_complete_date>=20180201 and pay_complete_date<20180301;'''
#关系类型
l_user_res='''relativetime'''
l_user_scene='''relativetime'''
#main方法执行
def main():
    l=loader()
    #生成参数
    #etl批次时间戳
    etltime=l.etltime()
    #执行
    l.batch_create_node(source,n_user,l_user,etltime)
    l.batch_create_node(source,n_res,l_res,etltime)
    l.batch_create_node(source,n_scene,l_scene,etltime)
    l.create_relationship(source,r_user_res,l_user,l_res,l_user_res)
    l.create_relationship(source,r_user_scene,l_user,l_scene,l_user_scene)
if __name__=='''__main__''':
    main()
