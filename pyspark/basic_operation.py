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




