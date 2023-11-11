
def with_rank(df, group_by_columns, order_by_columns, rank_column_name='rank'):
    if rank_column_name in df.columns:
        return df
    window = Window.partitionBy(*group_by_columns).orderBy(*order_by_columns)
    return df.withColumn(rank_column_name, F.row_number().over(window))



