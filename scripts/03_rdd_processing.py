import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("OlistRDD").master("local[2]")
         .config("spark.driver.memory", "2g")
         .config("spark.local.dir", "D:/spark-temp").getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# ---- Part A: create an RDD and clean it ----
raw = sc.textFile("../data/olist_order_payments_dataset.csv")
print("Total lines:", raw.count())
print("First 3 lines:", raw.take(3))

header = raw.first()
rows = raw.filter(lambda line: line != header).map(lambda line: line.split(","))
print("Data rows:", rows.count())

# ---- Part B: key-value processing ----
revenue_by_type = (rows.map(lambda r: (r[2], float(r[4])))
                       .reduceByKey(lambda a, b: a + b)
                       .sortBy(lambda kv: -kv[1]))
print("\nRevenue by payment type:")
for ptype, total in revenue_by_type.collect():
    print(f"  {ptype:12s} {total:15,.2f}")

count_by_type = rows.map(lambda r: (r[2], 1)).reduceByKey(lambda a, b: a + b)
print("\nPayments per type:", count_by_type.collect())

# ---- Part C: shared variables ----
limit = sc.broadcast(500.0)
big_payments = sc.accumulator(0)

def check(r):
    if float(r[4]) > limit.value:
        big_payments.add(1)

rows.foreach(check)
print(f"\nPayments above {limit.value}: {big_payments.value}")

spark.stop()