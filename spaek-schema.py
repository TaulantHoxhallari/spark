from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from pyspark import SparkConf

from lib.Log4j import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.appName("HelloSpark").master("local[3]").getOrCreate()


    data_struct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])  

    df = spark.read \
        .schema(data_struct)\
        .option("header", "true")\
            .option("inferSchema", "true")\
            .csv("data/flight-time.csv")  

    df.repartitionByRange(2)
    logger = Log4j(spark)
    df.select("OP_CARRIER").groupBy("OP_CARRIER").count().show()

    logger.warn(df.schema.simpleString())