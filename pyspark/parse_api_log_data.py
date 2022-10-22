#coding=utf-8

def parse_log_data(partitions):
    for row in partitions: # partiton
        if row.rsp.value is None or row.rsp.value.data is None or row.req.rec is None:
            continue
        if row.req.rec.extra is None or row.rsp.value.meta is None:
            continue
        if row.req.reqId is None:
            continue
        for item in row.rsp.value.data: # item
            yield {
                "req_id": row.req.reqId,
                "user_id": row.req.user.uid,
                "scene_id": row.req.rec.extra.scene_id,
                "scene": row.req.scene,
                "origin_item_id": item.idStr,
                "parent_item_id": row.req.rec.extra.parent_item_id,
                "item_id": item.id,
                "spm": row.req.rec.spm,
                "version": row.rsp.value.meta.version,
              	"datetime": row.datetime
            }


adf = spark.read.json("/data/kafka_dump/air_emall_1_20001181_api_log/20220905").repartition(1000).rdd.mapPartitions(parse_log_data).toDF()

#挑出来在cmcc_see_again场景看电视剧的用户的行为分析
adf2 = adf.filter("version like '%738%'")


#读取行为数据
bdf = spark.read.json("/yoda/1/offline/20001181/raw/stream_user_behavior_realtime_att/20220205")

#从bhv_trace_id中，去除掉hooray-前缀，得到req_id
from pyspark.sql.functions import substring
bdf = bdf.withColumn("req_id", substring(bdf.bhv_trace_id, 8, 100))

#行为数据过滤
bdf1 = bdf.filter(bdf.bhv_trace_id.isNotNull()).filter("bhv_scm='22'").filter("bhv_spm_1='电影详情页' or bhv_spm_1='电视剧详情页' or bhv_spm_1='综艺详情页'").filter("bhv_spm_2='box2' or bhv_spm_2='box5'")

#行为数据过滤，保留二期
bdf2 = bdf.filter(bdf.bhv_trace_id.isNotNull()).filter("bhv_scm='22'").filter("bhv_spm_1='搜索' or bhv_spm_1='搜索' or bhv_spm_1='列表页'").filter("bhv_spm_2='box4' or bhv_spm_2='box9' or bhv_spm_2='box1'")



cdf = bdf.filter("scene_id='1'") #「看过本片还喜欢」场景
cdf = bdf.filter("bhv_spm_2='box2'")
cdf = bdf.filter("scene_id='2'") #「同类热播」场景


from pyspark.sql.types import IntegerType, LongType
ddf = cdf.withColumn("item_id", cdf["item_id"].cast(LongType()))


#api拼接行为数据。 同时使用req_id/user_id/item_id作为key去做拼接，得到的曝光pv、点击pv跟ab报表上得到的大致对的上
rdf = adf.join(ddf, ["req_id","user_id","item_id"], "inner")
#rdf2 = adf.join(ddf.withColumnRenamed('user_id', 'bdf_user_id').withColumnRenamed('item_id', 'bdf_item_id'), ["req_id"], "inner")

rdf191 = rdf.where(rdf.version.contains('191'))
rdf192 = rdf.where(rdf.version.contains('192'))

#计算后验ctr
from pyspark.sql import functions as F
def func_post_ctr(bdf):
    xdf1 = bdf.filter("bhv_type='click'").groupBy("item_id").agg(F.count('bhv_type').alias('click_cnt'))
    xdf2 = bdf.filter("bhv_type='exposure'").groupBy("item_id").agg(F.count('bhv_type').alias('exposure_cnt'))
    xdf3 = xdf1.join(xdf2, "item_id", "inner")
    xdf3 = xdf3.withColumn('post_ctr', xdf3['click_cnt']/xdf3['exposure_cnt'])
    return xdf3

xdf1 = func_post_ctr(rdf191)
xdf2 = func_post_ctr(rdf192)
  
#读取媒资表
mdf = spark.read.parquet("/yoda/1/offline/20001181/merged_raw/goods_info/20220911")
col_list = ["item_id","item_name","video_type","tags","click_cnt","exposure_cnt","post_ctr"]
xdf12 = xdf1.join(mdf, "item_id", "inner").select(col_list)
xdf22 = xdf2.join(mdf, "item_id", "inner").select(col_list)
xdf12.orderBy('exposure_cnt', ascending=False).show(20)
xdf22.orderBy('exposure_cnt', ascending=False).show(20)



#
df=A.join(B, "user_id", "left_anti")



def func_split_spm(df):
    from pyspark.sql import functions as sf
    split_col = sf.split(df['spm'], '\\$##\\$')
    df2 = df.withColumn('spm1', split_col.getItem(0))
    return df2



