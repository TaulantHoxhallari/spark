from pyspark.sql import *
from pyspark import SparkConf
from lib.Log4j import Log4j
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Hello Spark").master("local[3]").getOrCreate()
    df = spark.read \
        .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("data/sample.csv")
    
  
    df.repartition(2)
    logger = Log4j(spark)
    
    df.select( "Country").where("Age <40").groupBy("Country").count().show()
    logger.warn("Hello spasrk ended")