from pyspark.sql import SparkSession

myspark = SparkSession.builder \
        .appName("sparksqoop") \
        .config("spark.driver.extraClassPath", \
         "/home/jdbc/ImpalaJDBC4.jar") \
	.config("spark.executor.extraClassPath", \
         "/home/jdbc/ImpalaJDBC4.jar") \
        .getOrCreate()

spark_context = myspark.sparkContext

#//for localhost database//
# df = myspark.read.jdbc(url=jdbcUrl, table="employees", column="emp_no", lowerBound=1, upperBound=100000, numPartitions=100)
df = myspark.read.jdbc("jdbc:impala://gromit:21050/default",table="stage" , column="myrowid" ,lowerBound=1, upperBound=1500000, numPartitions=13, properties={"user": "trex", "password": "password@123",  "driver":"com.cloudera.impala.jdbc4.Driver"})
print( "######################################")
print( "######################################")
print( "######################################")
print( "######################################")
print (df.count()) 
print( "######################################")
print( "######################################")
myrdd=df.rdd
print(df.take(10))
print( "######################################")
print( "######################################")

