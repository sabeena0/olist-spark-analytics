from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("OlistExplore").master("local[2]")
         .config("spark.driver.memory", "2g")
         .config("spark.local.dir", "D:/spark-temp").getOrCreate())

files = ["olist_orders_dataset", "olist_customers_dataset", "olist_order_items_dataset",
         "olist_order_payments_dataset", "olist_order_reviews_dataset",
         "olist_products_dataset", "olist_sellers_dataset",
         "olist_geolocation_dataset", "product_category_name_translation"]

for f in files:
    reader = spark.read.option("header", True).option("inferSchema", True)
    if "reviews" in f:
        reader = reader.option("multiLine", True).option("escape", '"')
    df = reader.csv(f"../data/{f}.csv")
    print(f"\n=== {f} ===  rows: {df.count()}")
    print(df.columns)

spark.stop()