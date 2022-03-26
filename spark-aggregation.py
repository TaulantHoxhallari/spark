from doctest import master
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName("SparkAggregation")\
        .master("local[3]")\
        .getOrCreate()

    # InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
    df = spark.read\
        .format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
                .load("data/invoices.csv")
    df.repartition(2)
    #df.printSchema()
    """ df.select(
        f.count("*").alias("Total items"),
        f.sum("Quantity").alias("Total quantity"),
        f.expr("round(sum(Quantity * UnitPrice), 2)").alias("Total incoming")
     ).show()

    df.selectExpr(
        " count(*) as `Total item` ",
        " sum(Quantity) as `Total quantity` ",
        " round(sum(Quantity * UnitPrice), 2) as `Total incoming` "
        ).show() """

    # df.createTempView("sales")
    # df = spark.sql("""
    #     select 
    #         Country, 
    #         InvoiceNo, 
    #         sum(Quantity) as Total,
    #         round(sum(Quantity * UnitPrice), 2) as InvoiceTotal
    #         from sales
    #         group by Country, InvoiceNo
    # """).show()

    report_df = df\
        .groupBy("Country", "InvoiceNo")\
        .agg(
            f.sum("Quantity").alias("TotalItems"), 
            f.round(f.expr("sum(Quantity * UnitPrice)"), 2).alias("InvoiceTotal")
        )
    report_df.show()

    # input("Pres enter to terminate")

    #df = spark.sql(" select * from sales ")
