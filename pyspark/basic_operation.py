#coding=utf-8
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,ArrayType
from pyspark.sql.functions import udf as udf



#Demo1: --------------------------------------------------------------------------
# df.groupBy后，使用agg函数
data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "time": "2022-11-06 12:02"}, 
    {"aid":22, "type":"t2", "pctr": 0.25, "time": "2022-11-06 12:02"},
    {"aid":33, "type":"t1", "pctr": 0.6, "time": "2022-11-06 10:02"},
    {"aid":66, "type":"t2", "pctr": 0.6, "time": "2022-11-06 11:02"}
]
df = spark.createDataFrame(data)
df.printSchema()

#求平均
stat = df.groupBy("type").agg(F.mean("pctr"))

#Demo2:---------------------------------------------------------------------------
# F.split切割字符串,
# |分割，则用\|切割
# $##$分割，则用\\$##\\$切割
data = [
    {"aid":11, "name":"aa|11"},
    {"aid":22, "name":"aa|22"},
    {"aid":33, "name":"bb|33"},
    {"aid":66, "name":"cc|66"}
]
df = spark.createDataFrame(data)
split_col = F.split(df['name'], '\|')
df2 = df.withColumn('name_1', split_col.getItem(0))

#Demo3:----------------------------------------------------------------------------
# F.expr将 json object 解开
data = [
    {"aid":11, "obj1": {"oid": '1111', "slot_id":2}},
    {"aid":66, "obj1": {"oid": '6666', "slot_id":9}},
    {"aid":33, "obj1": {"oid": '3333', "slot_id":6}},
    {"aid":99, "obj1": {"oid": '9999', "slot_id":8}},
    {"aid":88, "obj1": {"oid": '8888', "slot_id":1}},
]
df = spark.createDataFrame(data)
df2 = df.withColumn("obj_id", F.expr('obj1.oid').cast('string'))


#Demo4: --------------------------------------------------------------------------
# F.regexp_extract使用正则表达式,提取部分字符串
data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "ppsABTag": "aaa#perfmThr_Huoshan02#aaaaa"},
    {"aid":22, "type":"t2", "pctr": 0.25, "ppsABTag": "bbb#perfmThr_Huoshan01#bbbb"},
    {"aid":33, "type":"t1", "pctr": 0.6, "ppsABTag": "cc#perfmThr_Huoshan02#ccc"},
    {"aid":66, "type":"t2", "pctr": 0.6, "ppsABTag": "dd#perfmThr_Huoshan01#ddd"}
]
df = spark.createDataFrame(data)
df2 = df.withColumn('ab_version', F.regexp_extract('ppsABTag', r'(perfmThr.*?)(?=#)', 1))

#Demo5:-------------------------------------------------------------------------
#时间戳转北京时间
from pyspark.sql import functions as F
data = [
    {"aid":11, "req_time": 1670577463},
    {"aid":22, "req_time": 1670573538},
    {"aid":33, "req_time": 1670559140},
    {"aid":66, "req_time": 1670556740}
]
df = spark.createDataFrame(data)
df2 = df.withColumn("bj_time", F.from_unixtime(F.col("req_time")))


#Demo6:-------------------------------------------------------------------------
#当前行与上一行求差F.when
from pyspark.sql import functions as F
from pyspark.sql.window import Window

data = [
    {"aid":11, "req_time": 1670577463, "value": 86},
    {"aid":22, "req_time": 1670573538, "value": 12},
    {"aid":33, "req_time": 1670559140, "value": 98},
    {"aid":66, "req_time": 1670556740, "value": 32}
]
df = spark.createDataFrame(data)
my_window = Window.partitionBy().orderBy("req_time")
df = df.withColumn("prev_value", F.lag(df.value).over(my_window))
df = df.withColumn("diff", F.when(F.isnull(df.value - df.prev_value), 0).otherwise(df.value - df.prev_value))















