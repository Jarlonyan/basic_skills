#coding=utf-8
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,ArrayType
from pyspark.sql.functions import udf as udf

data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "time": "2022-11-06 12:02"}, 
    {"aid":22, "type":"t2", "pctr": 0.25, "time": "2022-11-06 12:02"},
    {"aid":33, "type":"t1", "pctr": 0.6, "time": "2022-11-06 10:02"},
    {"aid":66, "type":"t2", "pctr": 0.6, "time": "2022-11-06 11:02"}
]

df = spark.createDataFrame(data)

df.printSchema() #打印schema

#求平均
stat = df.groupBy("type").agg(F.mean("pctr"))


#collesce的用法

#Demo3: F.expr将 json object 解开
data = [
    {"aid":11, "obj1": {"oid": '1111', "slot_id":2}},
    {"aid":66, "obj1": {"oid": '6666', "slot_id":9}},
    {"aid":33, "obj1": {"oid": '3333', "slot_id":6}},
    {"aid":99, "obj1": {"oid": '9999', "slot_id":8}},
    {"aid":88, "obj1": {"oid": '8888', "slot_id":1}},
]
df = spark.createDataFrame(data)

df2 = df.withColumn("obj_id", F.expr('obj1.oid').cast('string'))




#Demo4: F.regexp_extract使用正则表达式,提取部分字符串
data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "ppsABTag": "aaa#perfmThr_Huoshan02#aaaaa"},
    {"aid":22, "type":"t2", "pctr": 0.25, "ppsABTag": "bbb#perfmThr_Huoshan01#bbbb"},
    {"aid":33, "type":"t1", "pctr": 0.6, "ppsABTag": "cc#perfmThr_Huoshan02#ccc"},
    {"aid":66, "type":"t2", "pctr": 0.6, "ppsABTag": "dd#perfmThr_Huoshan01#ddd"}
]
df = spark.createDataFrame(data)
df2 = df.withColumn('ab_version', F.regexp_extract('ppsABTag', r'(perfmThr.*?)(?=#)', 1))




