from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName("SparkAggregation")\
        .master("local[3]")\
        .getOrCreate()

    df = spark.read\
        .format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
                .load("data/invoices.csv")
    df.repartition(2)


    report_df = df\
        .groupBy("Description")\
        .agg(
            f.sum("Quantity").alias("Sold quantity")
        ).orderBy("Description")
   


    report_df.show()