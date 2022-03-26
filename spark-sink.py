from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from pyspark import SparkConf

from lib.Log4j import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkSql").master("local[3]").getOrCreate()


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
                .option("dateFormat", "M/d/y")\
            .csv("data/flight-time.csv") 

    df.repartition(2)

    df.createOrReplaceTempView("df_ref") 
    result = spark.sql(" select FL_DATE, count(1) as total_flights from df_ref where OP_CARRIER = 'UA' group by FL_DATE ")
    result.show()

    