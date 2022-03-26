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
        .groupBy("Country", "InvoiceNo" ,"InvoiceDate")\
        .agg(
            f.sum("Quantity").alias("TotalItems"), 
            f.round(f.expr("sum(Quantity * UnitPrice)"), 2).alias("InvoiceTotal")
        ).orderBy("Country")

    accumulating_total_window = Window.partitionBy("Country")\
        .orderBy("InvoiceDate")\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    report_df=report_df.withColumn(
        "AccumulatingTotal", 
        f.sum("InvoiceTotal").over(accumulating_total_window)
        )

    report_df.show()
