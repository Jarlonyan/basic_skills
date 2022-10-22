#coding=utf-8
#读取user_profile数据，然后统计每个slot的覆盖率

data = [
    {"aid":11, "fids":"725900317002156569 72590031700215650"}, 
    {"aid":22, "fids":"725900317002156559 725900317002156558 725900317002156559"},
    {"aid":33, "fids":"625900317002156569 785900317002156569"}
]

df = spark.createDataFrame(data)


from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,ArrayType
from pyspark.sql.functions import udf as udf

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
rdf = df3.select("fid_tmp", tmp_udf("fid_tmp")).withColumnRenamed("<lambda>(fid_tmp)", "slots")


from pyspark.sql.functions import collect_set
rdf.agg(collect_set('slots')).collect()





