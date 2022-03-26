from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
import pyspark.sql.functions as func

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
        .groupBy("InvoiceNo" ,"InvoiceDate")\
        .agg(
            f.sum("Quantity").alias("TotalItems")
        ).orderBy("InvoiceDate")

    
    report_df = report_df.filter(func.col("TotalItems") > 0)

    report_df.show()