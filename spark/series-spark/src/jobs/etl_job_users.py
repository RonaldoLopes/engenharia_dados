import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *


# main definitions & calls
def main():
    spark = SparkSession.builder.appName("etl-job-users").getOrCreate()

    # configs
    print(spark)
    print(SparkConf().getAll())
    spark.sparkContext.setLogLevel("INFO")

    # extract {E}
    users_filepath = '/home/ronaldo/engenharia_dados/spark/series-spark/docs/users.json'
    df_users = spark.read.json(users_filepath)
    df_users.show()

    # transform {T}
    df_rank = df_users.select(expr("CASE WHEN useful >= 500 THEN 'high' " + "ELSE 'low' END").alias('score'))
    df_rank.show()
    
    # load {L}
    df_rank.write.mode("overwrite").parquet('/home/ronaldo/engenharia_dados/spark/series-spark/docs/output/')


# entry point for pyspark etl app
if __name__ == '__main__':
    main()
