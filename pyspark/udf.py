#coding=utf-8
#读取user_profile数据，然后统计每个slot的覆盖率

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,ArrayType,StringType,LongType,StructType
from pyspark.sql.functions import udf as udf

#Demo1:-------------------------------------------------------------------------------------------
#对df某一列操作，提取出slot_id，并
data = [
    {"aid":11, "fids":"725900317002156569 72590031700215650"}, 
    {"aid":22, "fids":"725900317002156559 725900317002156558 725900317002156559"},
    {"aid":33, "fids":"625900317002156569 785900317002156569"}
]
df = spark.createDataFrame(data)
df.printSchema() #打印schema

df2 = df.select(F.split('fids',' ').alias('fid_list'))
df3 = df2.withColumn("fid_tmp", df2.fid_list.cast("array<long>"))

def func_udf(col):
	res = []
	for a in col:
		res.append(a>>54)
	return res

def func_udf2(col):
	res = set()
	for a in col:
		res.add(a>>54)
	return list(res)

tmp_udf = udf(lambda x:func_udf2(x), ArrayType(IntegerType()))
rdf = df3.withColumn("slots", tmp_udf("fid_tmp"))

from pyspark.sql.functions import collect_set
rdf.agg(collect_set('slots')).collect()

#Demo2:-------------------------------------------------------------------------------------------
#针对某一列使用udf提取字符串。ppsABTag='111#222#perfmThr_Huoshan01#888'，
#从这里面提取出来perfmThr_作为新的一列ab_version
data = [
    {"aid":11, "type":"t1", "pctr": 0.2, "ppsABTag": "aaa#perfmThr_Huoshan02#aaaaa"},
    {"aid":22, "type":"t2", "pctr": 0.25, "ppsABTag": "bbb#perfmThr_Huoshan01#bbbb"},
    {"aid":33, "type":"t1", "pctr": 0.6, "ppsABTag": "cc#perfmThr_Huoshan02#ccc"},
    {"aid":66, "type":"t2", "pctr": 0.6, "ppsABTag": "dd#perfmThr_Huoshan01#ddd"}
]
df = spark.createDataFrame(data)

def process_ab_version_from_ppsABTag(df):
    def func_get_abversion(col):
        ab_version = ''
        for a in col:
            if u'perfmThr_' in a:
                ab_version = a
                break
        return ab_version
    udf_func_get_ab = udf(lambda x:func_get_abversion(x), StringType())
    df2 = df.withColumn('ABTag_list', split(col('ppsABTag'), "#"))
    df3 = df2.withColumn("ab_version", udf_func_get_ab("ABTag_list"))
    return df3
df2 = process_ab_version_from_ppsABTag(df)


#Demo3:--------------------------------------------------------------------------------------------
#df.groupBy后，在agg中使用udf函数
data = [
    {"aid":11, "value":"a"},
    {"aid":11, "value":"b"},
    {"aid":11, "value":"a"},
    {"aid":22, "value":"c"},
]
df = spark.createDataFrame(data)
df.groupBy('aid').agg(F.collect_list('value').alias('value_list')).show()


def find_a(x):
  """Count 'a's in list."""
  output_count = 0
  for i in x:
    if i == 'a':
      output_count += 1
  return output_count

find_a_udf = F.udf(find_a, T.IntegerType())
find_a_udf = F.udf(lambda x: find_a(x), T.IntegerType())
df.groupBy('aid').agg(find_a_udf(F.collect_list('value')).alias('a_count')).show()


#Demo4: -------------------------------------------------------------------------------------------
#df.groupBy后，udf作用在2列上

data = [
    {"aid":11, "col_a":"t1", "col_b": 1},
    {"aid":22, "col_a":"t2", "col_b": 5},
    {"aid":11, "col_a":"t3", "col_b": 6},
    {"aid":22, "col_a":"t4", "col_b": 6}
]
df = spark.createDataFrame(data)

def func_two_cols(cols_a, cols_b):
    res = ''
    for x,y in zip(cols_a, cols_b):
        res += str(x)+str(y)
    return res

udf_func_two_cols = udf(lambda x,y: func_two_cols(x, y), StringType())
df.groupBy('aid').agg(udf_func_two_cols(F.collect_list('col_a'),F.collect_list('col_b')).alias('mix_cols')).show()


#Demo5: -----------------------------------------------------------------------------------------------
#原始df中fids是一个string，空格隔开，用空格切分，每个fid转成Long类型，然后提取slot=36的fid

data = [
    {"aid":11, "fids":"725900317002156569 72590031700215650"},
    {"aid":22, "fids":"725900317002156559 725900317002156558 725900317002156559"},
    {"aid":33, "fids":"625900317002156569 785900317002156569"}
]
df = spark.createDataFrame(data)

def func_get_ssp_id(col):
    res = -1
    for fid in col:
        slot_id  = fid >> 54
        if slot_id == 36:
            res = fid
            return res
    return res
udf_func_sspid = F.udf(lambda x:func_get_ssp_id(x), LongType())

def get_ssp_fid(df):
    df2 = df.withColumn('fid_list_tmp', F.split('fids', ' '))
    df3 = df2.withColumn('fid_list', df2.fid_list_tmp.cast('array<long>'))
    df4 = df3.withColumn('ssp_id_fid', udf_func_sspid('fid_list'))
    return df4

#Demo6:-------------------------------------------------------------------------------------------
#使用udf对一列进行计算，结果得到2列
import pyspark.sql.functions as F
from pyspark.sql.types import *

data = [
    {"aid":11, "col_a":"t1", "col_b": 1},
    {"aid":22, "col_a":"t2", "col_b": 5},
    {"aid":11, "col_a":"t3", "col_b": 6},
    {"aid":22, "col_a":"t4", "col_b": 6}
]
df = spark.createDataFrame(data)

schema = StructType([
    StructField("res1", IntegerType(), False),
    StructField("res2", IntegerType(), False)
])

def func_calc_2_res(col):
    res  = [None, None]
    if col is not None:
        res = [col/2, col%2]
    return res

udf_calc_2_res = F.udf(func_calc_2_res, schema)
df2 = df.withColumn("tmp", udf_calc_2_res("col_b"))
df3 = df2.withColumn('res1', F.expr('tmp.res1')).withColumn('res2', F.expr('tmp.res2'))

#Demo7------------------------------------------------------------------




