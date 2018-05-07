#!/usr/bin/env python
# -*- coding: utf-8 -*
from import_helper import loader

##参数
#数据源类型
source='meta'
#database节点
q_database='select DB_ID,`DESC`,DB_LOCATION_URI,NAME as DB_NAME,OWNER_NAME,OWNER_TYPE,NAME as name,DB_ID as pid from h_dat_hive.dbs'
l_database='database'
#table节点
q_table='select t1.*,t1.TBL_NAME as name,md5(concat(t2.NAME,t1.TBL_NAME)) as pid from h_dat_hive.tbls t1 join h_dat_hive.dbs t2 on t1.DB_ID=t2.DB_ID'
l_table='table'
#column节点
q_column='select *,COLUMN_NAME as name,md5(concat(CD_ID,COLUMN_NAME)) as pid from h_dat_hive.columns_v2'
l_column='column'
#table_database关系
q_table_database='select md5(concat(t2.NAME,t1.TBL_NAME)) as pid,t2.DB_ID as pid_e from h_dat_hive.tbls t1 join h_dat_hive.dbs t2 on t1.DB_ID=t2.DB_ID'
l_table='table'            # 节点标签
l_database='database'      # 尾部节点标签
t_table_database='extend {name:"extend"}'       # 关系类型
#column_table关系
q_column_table='select md5(concat(t1.CD_ID,t1.COLUMN_NAME)) as pid,md5(concat(t3.NAME,t2.TBL_NAME)) as pid_e from h_dat_hive.columns_v2 t1 join h_dat_hive.tbls t2 on t1.CD_ID=t2.TBL_ID join h_dat_hive.dbs t3 on t2.DB_ID=t3.DB_ID'
l_column='column'          # 节点标签
l_table='table'            # 尾部节点标签
t_column_table='extend {name:"extend"}'         # 关系类型

#main方法执行
def main():
    l=loader()
    ##生成参数
    #etl批次时间戳
    etltime=l.etltime()
    ##执行
    l.merge_node(source,q_database,l_database,etltime)
    l.merge_node(source,q_table,l_table,etltime)
    l.merge_node(source,q_column,l_column,etltime)
    l.merge_relationship(source,q_table_database,l_table,l_database,t_table_database)
    l.merge_relationship(source,q_column_table,l_column,l_table,t_column_table)
if __name__=='''__main__''':
    main()
