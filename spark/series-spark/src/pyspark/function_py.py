import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("py-functions").getOrCreate()
print("======================================")
print(SparkConf().getAll())
spark.sparkContext.setLogLevel("ERROR")

loc_users = '/home/ronaldo/engenharia_dados/spark/series-spark/docs/users.json'
loc_business = '/home/ronaldo/engenharia_dados/spark/series-spark/docs/business.json'

df_users = spark.read.json(loc_users)
df_business = spark.read.json(loc_business)

#generate id = monotonically_increasing_id
df_users.select(monotonically_increasing_id().alias('event'), 'name').show(2)

#greatest = greatest value of the list
df_users.select('name', 'compliment_cool', 'compliment_cute',
                'compliment_funny', 'compliment_hot',
                greatest('compliment_cool', 'compliment_cute',
                'compliment_funny', 'compliment_hot').alias('highest_rate')).show()

#expr = expression string into the column that ir represents {sql-like expressions}
df_users.select(expr("CASE WHEN useful >= 500 THEN 'high' " + "ELSE 'low' END").alias('score')).show()

from pyspark.sql.functions import upper

def verify_rank(df):
    return df_users.withColumn('rank', when(col('useful') >= 500, lit('high')).otherwise(lit('low')))

df_users.transform(verify_rank).show()


