#!/usr/bin/env python
# -*- coding: utf-8 -*
from import_helper import loader
from agent import *
from os import *
from datetime import datetime
##参数
#数据源类型
source="local"
#job
q_job="call neo4j_job.job_meta_neo4j()"
l_job="job"
#table_job关系
q_table_job="select pid,pid_e from neo4j_job.job_meta_source"  # 从视图获取
l_job="job"
l_table="table"
t_source='''following {name:"source"}'''
#job_table关系
q_job_table="select pid,pid_e from neo4j_job.job_meta_target"
l_table="table"
l_job="job"
t_target='''following {name:"target"}'''

#main方法执行
def main():
    filepath="/usr/local/datamap/job/bigdata-dwh/code/"
    suffix="sql"                # 脚本后缀.sql
    os.chdir(filepath)
    pull=os.popen("git pull --rebase origin dev")
    print(pull.read())
    a=agent()
    a.take_action(filepath,suffix)
    l=loader()
    ##生成参数
    #etl批次时间戳
    etltime=l.etltime()
    ##执行
    l.merge_node(source,q_job,l_job,etltime)
    l.merge_relationship(source,q_table_job,l_table,l_job,t_source)
    l.merge_relationship(source,q_job_table,l_job,l_table,t_target)
    add=os.popen("git add .")
    print(add.read())
    commit=os.popen('''git commit -m "neo4j has finished the work"''')
    print(commit.read())
    push=os.popen("git push -u origin master:dev")
    print(push.read())
    print(datetime.now(),'''job注解获取完成''')
if __name__=='''__main__''':
    main()
