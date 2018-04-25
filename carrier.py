#!/usr/bin/env python
# -*- coding: utf-8 -*
import pymysql
import ConfigParser
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
        cf = ConfigParser.RawConfigParser()
        cf.read("./neo4j_link.conf")
        uri=cf.get("portal", "uri")
        username=cf.get("portal", "username")
        password=cf.get("portal", "password")
        driver=GraphDatabase.driver(uri, auth=(username, password))
        return driver

    def run_cypher(self,cypher):  # 运行cypher
        driver=self.get_driver()
        with driver.session() as session:
            with session.begin_transaction() as tx:
                run=tx.run(cypher)
                return run

