from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("OlistLoad")
         .master("local[2]")
         .config("spark.driver.memory", "2g")
         .config("spark.local.dir", "D:/spark-temp")
         .getOrCreate())

orders = spark.read.csv("../data/olist_orders_dataset.csv", header=True, inferSchema=True)

orders.printSchema()
orders.show(5, truncate=False)
print("Rows:", orders.count())

spark.stop()