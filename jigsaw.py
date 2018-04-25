#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *

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
        batch='''{batchsize:10000,parallel:true,iteratelist:true}'''
        cypher=f'''
        CALL apoc.periodic.iterate(
        'call apoc.load.jdbc("{source}","{query}") YIELD row'
        ,'create(n:outside:{label}) set n=row, n.etltime="{etltime}"'
        ,{batch}
        )
        '''
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

    def batch_relationship(self,source,query,label,label_end,relationtype):  # 抽取关系数据(source:数据来源类型,query:查询语句,label:节点标签,label_end:尾部节点标签,relationtype:关系类型)
        check='''{pid:row.pid}'''
        check_e='''{pid:row.pid_e}'''
        relationtype=relationtype+''' '''+'''{row}'''
        batch='''{batchsize:2000,parallel:false,iteratelist:false}'''
        cypher=f'''
        CALL apoc.periodic.iterate(
        'call apoc.load.jdbc("{source}","{query}") YIELD row '
        ,'merge (n:outside:{label} {check}) with *
        merge (n_e:outside:{label_end} {check_e}) with *
        create (n)-[r:{relationtype}]->(n_e)'
        ,{batch}
        )
        '''
        print(cypher)
        c=carrier()
        tx=c.run_cypher(cypher)
        return cypher

