from select import select
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName("SparkAggregation")\
        .master("local[3]")\
        .getOrCreate()

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")
    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    #order_df.show()
    #product_df.show()
    
    join_on =  order_df.prod_id == product_df.prod_id
    
    # Naming problem: First solution
    """ result_df = order_df\
        .drop(order_df.qty)\
        .join(product_df, join_on, "outer")\
        .select("prod_name", "qty") """

    # Naming problem: Second solution
    product_qty_renamed = product_df.withColumnRenamed("qty", "product_qty")
    result_df = order_df\
        .join(product_qty_renamed, join_on, "left")\
        .drop(product_qty_renamed.prod_id)\
        .select(
            "order_id", 
            "prod_id", 
            "prod_name", 
            "unit_price",
            "list_price",
            "product_qty"
        )\
            .withColumn("prod_name", expr("coalesce(prod_name, prod_id)"))\
            .withColumn("list_price", expr("coalesce(list_price, unit_price)"))\
            .sort("prod_name")
    result_df.show()