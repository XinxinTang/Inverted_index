from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, collect_list



spark = SparkSession.builder.appName("test_spark").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

df_new = spark.read.option("header","true").csv("/usr/apps/result.csv")
df_res = df_new.groupby("wordId").agg(collect_list("docId").alias("docId_list"))
df_res = df_res.sort(['wordId'])
df_res = df_res.withColumn('docId_list', concat_ws(',', 'docId_list'))
df_res.show(truncate=False)
df_res.coalesce(1).write.option("header", "true").mode("overwrite").csv("/inverted_index.csv")

spark.stop()