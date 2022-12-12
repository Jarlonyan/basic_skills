#coding=utf-8
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,ArrayType
from pyspark.sql.functions import udf as udf


#Demo1:-------------------------------------------------------------------------------
#一些基础操作
#dataframe读取
df = spark.read.json("/xxxxx")
df = spark.read.text("/xxxxx")
df = spark.read.parquet("/xxxxx")
df = spark.read.orc("/xxxxx")

#排序
df.orderBy('exposure_cnt', ascending=False).show(50,False)

#过滤
df.filter("name='hehe'") #保留等于hehe的行
df.filter("name like '%Huoshan%'")
df.filter(df.page_name.isin('综艺','动漫','纪录片','教育','会员'))
df.filter(df.name.isNotNull())) #保留name非空的，对应的空是：df.name.isNull()
df.where(df.name.contains('191'))

#类型转换
df = df.withColumn("item_id", df["item_id"].cast(LongType()))

#重命名
df = df.withColumnRenamed('item_id', 'new_item_id')

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
#用F.when条件，求当前行与上一行求差
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


#Demo7: -----------------------------------------------------------------------
#各种join
rdf = adf.join(bdf, "item_id", "inner")
rdf = adf.join(bdf, ["req_id", "item_id"], "inner") #两个key来join
rdf = adf.join(bdf, "item_id", "left_anti") #在adf中，但不在bdf中
rdf = adf.join(bdf, "item_id", "left_semi") #left_semi用于返回跟左表一样schema的结果，最终结果是inner的结果


#Demo8-----------------------------------------------------------------------------
#df某列是string类型，但是可以解析成json，使用F.from_json从json string中解析字段
data = [
    {"aid":11, "extra_info": json.dumps({"oid": '1111', "slot_id":2})},
    {"aid":66, "extra_info": json.dumps({"oid": '6666', "slot_id":9})},
    {"aid":33, "extra_info": json.dumps({"oid": '3333', "slot_id":6})},
    {"aid":99, "extra_info": json.dumps({"oid": '9999', "slot_id":8})},
    {"aid":88, "extra_info": json.dumps({"oid": '8888', "slot_id":1})}
]
df = spark.createDataFrame(data)

json_schema = spark.read.json(df.rdd.map(lambda row: row.extra_info)).schema
df2 = df.withColumn('extra_info', F.from_json(F.col('extra_info'), json_schema))
df3 = df2.withColumn('extra_info_id', F.col('extra_info.oid'))

#Demo9:-------------------------------------------------------------------------
#使用F.substring截取子字符串
df2 = df.withColumn("second_name", F.substring(df.name, 6, 10))

#Demo10:----------------------------------------------------------------------
#使用F.lit填充默认值
df2 = df.withColumn("status", F.lit(0)) #填充





