# sql2neo4j
从关系型数据导入数据到neo4j
## 连接
工具内部使用apoc来实现导入功能
### neo4j连接配置
在配置文件中设置neo4j的连接信息  
``sql2neo4j/neo4j_link.conf``
### mysql连接配置
在neo4j的配置文件中设置mysql连接(通过apoc):  
配置文件``neo4j/conf/neo4j.conf``中新增一行:  
``apoc.jdbc.local.url=jdbc:mysql://127.0.0.1:3306/s_demo?user=testusername&password=testpassword``  
``source='local'``local就是source参数的值
## 导入
mysql表规范:  
导入工具内部使用pid,pid_e来关联关系与节点  
1.每张表保存一个节点/关系的数据  
2.对于存储节点的表``pid``字段作为主键  
3.对于存储关系的表``pid``字段作为起点的主键,``pid_e``字段作为终点的主键  
在自己的代码中引用模块:  
``from import_helper import loader``  
方法:说明  
 ``etltime()``:生成etl时间  
 ``merge_node()``:同步节点,如果有pid相同的节点,将更新节点  
 ``merge_relationship()``:同步关系,如果pid相同的节点中,存在相同的关系类型,将更新关系  
 ``create_node()``:导入节点  
 ``create_relationship()``:导入关系  
 ``batch_create_node()``:分批导入节点,对于数据量千万级以上的节点,建议分批导入  
 ``batch_create_relationship()``:分批导入关系  
### 节点导入参数
 ``source:mysql连接,query:sql语句,label:标签,etltime:etl时间``  
 例如:  
 ``create_node(local,"select * from test.person","person","2018-01-01 00:00:00")``  
 ``create_node(local,"select * from test.book","book","2018-01-01 00:00:00")``  
### 关系导入参数
 ``source:mysql连接,query:sql语句,label:关系的起点标签,label_end:关系的终点标签,relationtype:关系类型``  
 例如:  
 ``create_relationship(local,"select * from test.person_like_book","person","book","2018-01-01 00:00:00")``
## 参考
样例代码:  
``sql2neo4j/load_demo.py``
``sql2neo4j/load_demo_desc.sql``
