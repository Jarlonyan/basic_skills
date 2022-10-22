#coding=utf-8
bdf = spark.read.json("/yoda/1/offline/20001181/raw/stream_user_behavior_realtime/20220418")
from pyspark.sql.functions import from_json, col
json_schema = spark.read.json(bdf.rdd.map(lambda row: row.extra_info)).schema
bdf2=bdf.withColumn('extra_info', from_json(col('extra_info'), json_schema))


bdf3=bdf2.select(col('extra_info.api_id').alias('api_id'), col('extra_info.sys_id').alias('sys_id'), col('extra_info.flag_play_type').alias('extra_bhv_type'), "user_id", "trace_id", "scm", "origin_item_id", "spm", "bhv_type")


bdf4 = bdf3.filter("sys_id='联通'")
from pyspark.sql import functions as sf
split_col = sf.split(bdf4['spm'], '\\$##\\$')
bdf5 = bdf4.withColumn('spm1', split_col.getItem(0))
bdf6 = bdf5.withColumn('spm2', split_col.getItem(1))
bdf7 = bdf6.filter("spm1='列表页'")


#页面访问
bdf_page=bdf7.filter("column_name='综艺' or column_name='动漫' or column_name='纪录片' or column_name='教育' or column_name='会员'")


#曝光、点击、播放、订购
bdf21 = bdf7.filter("scm='21'").filter("api_id='人工推荐'")
bdf22 = bdf7.filter("scm='22'").filter("spm2='box1'").filter(bdf7.trace_id.isNotNull()).filter("api_id='10.11.191.33:10008'")
bdf23 = bdf7.filter("scm='23'").filter("spm2='box1'").filter(bdf7.trace_id.isNotNull()).filter("api_id='10.11.191.33:10007'")
bdf24 = bdf7.filter("scm='24'").filter("spm2='box1'").filter(bdf7.trace_id.isNotNull()).filter("api_id='10.11.191.33:10012'")

bdf22.groupBy("extra_bhv_type").count().orderBy('count', ascending=False).show()
bdf22.groupBy("bhv_type").count().orderBy('count', ascending=False).show()


