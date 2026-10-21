import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data").replace("\\", "/") + "/"


def get_spark(app_name="Olist"):
    spark = (SparkSession.builder.appName(app_name).master("local[2]")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.local.dir", "D:/spark-temp")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_tables(spark):
    def read(name, **opts):
        r = spark.read.option("header", True).option("inferSchema", True)
        for k, v in opts.items():
            r = r.option(k, v)
        return r.csv(DATA + name + ".csv")

    return {
        "orders": read("olist_orders_dataset"),
        "customers": read("olist_customers_dataset"),
        "items": read("olist_order_items_dataset"),
        "payments": read("olist_order_payments_dataset"),
        "reviews": read("olist_order_reviews_dataset", multiLine=True, escape='"'),
        "products": read("olist_products_dataset"),
        "sellers": read("olist_sellers_dataset"),
        "category": read("product_category_name_translation"),
    }