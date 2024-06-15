

#with_rank解析1
def with_rank(df, group_by_columns, order_by_columns, rank_column_name='rank'):
    if rank_column_name in df.columns:
        return df
    window = Window.partitionBy(*group_by_columns).orderBy(*order_by_columns)
    return df.withColumn(rank_column_name, F.row_number().over(window))

cvr_positive_instance_df = with_rank(self.ctr_convert_instance_data.df.where('is_positive'),
                                     instance_pk,
                                     [F.desc('time')]).where('rank = 1').drop('rank').withColumn('label', F.lit(1))\
                            .join(self.positive_label_data.df.select(*instance_pk).dropDuplicates(), instance_pk)


#with rank解析2
# you can order by the column you prefer, not only id
from pyspark.sql import functions as F, Window
w = Window.partitionBy('vehicle', 'EU_variant').orderBy('id')
df.withColumn(
    'ECU_Variant_rank', 
    F.concat_ws('', F.col('EU_variant'), F.lit('('), F.rank().over(w), F.lit(')'))
)


