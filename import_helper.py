#!/usr/bin/env python
# -*- coding: utf-8 -*
import pymysql
import configparser
import time
from datetime import datetime
from neo4j.v1 import GraphDatabase


class carrier(object):

    def __init__(value):
        value=value

    def run_query(self,query):               # 执行sql,获取数据
        conn=self.get_connect()
        cursor=conn.cursor()  # 使用 cursor() 方法创建一个游标对象 cursor
        try:
            cursor.execute(query)         # 执行SQL语句
            desc=cursor.description
            column=[]
            for field in desc:
                column.append(field[0])
            results=[]
            results.append(column)
            results.extend(cursor.fetchall())  # 获取所有记录列表
            return results      # 返回results
        except Exception as error:
            print ("错误信息:",error)
        finally:
            cursor.close()          # 关闭cursor
            conn.commit()           # 提交
            conn.close()            # 关闭conn

    def execute_many(self,sql,data):               # 执行sql,获取数据
        conn=self.get_connect()
        cursor=conn.cursor()  # 使用 cursor() 方法创建一个游标对象 cursor
        try:
            results=cursor.executemany(sql,data)   # 执行SQL语句
            return results      # 返回results
        except Exception as error:
            print ("错误信息:",error)
        finally:
            cursor.close()          # 关闭cursor
            conn.commit()           # 提交
            conn.close()            # 关闭conn

    def get_driver(self):       # 获取neo4j的session
        cf=configparser.ConfigParser()
        cf.read("./neo4j_link.conf")
        uri=cf.get("conf","uri")
        username=cf.get("conf","username")
        password=cf.get("conf","password")
        driver=GraphDatabase.driver(uri, auth=(username, password))
        return driver

    def run_cypher(self,cypher):  # 运行cypher
        driver=self.get_driver()
        with driver.session() as session:
            with session.begin_transaction() as tx:
                run=tx.run(cypher)
                return run


class jigsaw(object):

    def __init__(value):
        value=value

    def extract_node(self,source,query,label,etltime):  # 抽取节点数据(source:数据来源类型,query:查询语句,label:节点标签,etltime:etl时间)
        check='''{pid:row.pid}'''

        cypher=f'''
        call apoc.load.jdbc("{source}","{query}") YIELD row
        merge(n:outside:{label} {check}) set n=row, n.etltime="{etltime}"
        '''
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

    def extract_relationship(self,source,query,label,label_end,relationtype):  # 抽取关系数据(source:数据来源类型,query:查询语句,label:节点标签,label_end:尾部节点标签,relationtype:关系类型)
        check='''{pid:row.pid}'''
        check_e='''{pid:row.pid_e}'''
        cypher=f'''
        call apoc.load.jdbc("{source}","{query}") YIELD row
        merge (n:outside:{label} {check}) with *
        merge (n_e:outside:{label_end} {check_e}) with *
        merge (n)-[r:{relationtype}]->(n_e) set r+=row
        '''
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

    def load_node(self,source,query,label,etltime):  # 创建节点(source:数据来源类型,query:查询语句,label:节点标签,etltime:etl时间)
        cypher=f'''
        call apoc.load.jdbc("{source}","{query}") YIELD row
        create(n:outside:{label}) set n=row, n.etltime="{etltime}"
        '''
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

    def load_relationship(self,source,query,label,label_end,relationtype):  # 创建关系(source:数据来源类型,query:查询语句,label:节点标签,label_end:尾部节点标签,relationtype:关系类型)
        check='''{pid:row.pid}'''
        check_e='''{pid:row.pid_e}'''
        cypher=f'''
        call apoc.load.jdbc("{source}","{query}") YIELD row
        merge (n:outside:{label} {check}) with *
        merge (n_e:outside:{label_end} {check_e}) with *
        create (n)-[r:{relationtype}]->(n_e) set r+=row
        '''
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

    def batch_node(self,source,query,label,etltime):  # 抽取节点数据(source:数据来源类型,query:查询语句,label:节点标签,etltime:etl时间)
        check='''{pid:row.pid}'''
        batch='''{batchsize:10000,parallel:false,iteratelist:true}'''
        cypher=f'''
        CALL apoc.periodic.iterate(
        'call apoc.load.jdbc("{source}","{query}") YIELD row '
        ,'merge(n:outside:{label} {check}) set n=row, n.etltime="{etltime}" '
        ,{batch}
        )
        '''
        print(cypher)
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

    def batch_relationship(self,source,query,label,label_end,relationtype):  # 抽取关系数据(source:数据来源类型,query:查询语句,label:节点标签,label_end:尾部节点标签,relationtype:关系类型)
        check='''{pid:row.pid}'''
        check_e='''{pid:row.pid_e}'''
        relationtype=relationtype
        batch='''{batchsize:10000,parallel:false,iteratelist:true}'''
        cypher=f'''
        CALL apoc.periodic.iterate(
        'call apoc.load.jdbc("{source}","{query}") YIELD row '
        ,' merge (n:outside:{label} {check}) with *
        merge (n_e:outside:{label_end} {check_e}) with *
        merge (n)-[r:{relationtype}]->(n_e) set r.freq = coalesce(r.freq,0)+1,r+=row '
        ,{batch}
        )
        '''
        print(cypher)
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher


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
