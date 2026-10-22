import os
from common import HERE, get_spark, load_tables
from pyspark.sql import functions as F

spark = get_spark("LateByMonth")
t = load_tables(spark)

late = (t["orders"]
    .filter((F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNotNull())
    .withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    .withColumn("is_late", (F.datediff("order_delivered_customer_date",
                                       "order_estimated_delivery_date") > 0).cast("int")))

monthly = (late.groupBy("month")
    .agg(F.count("*").alias("orders"),
         F.round(100 * F.avg("is_late"), 1).alias("late_pct"))
    .orderBy("month"))
monthly.show(30)

monthly.toPandas().to_csv(os.path.join(HERE, "..", "output", "late_by_month.csv"), index=False)
spark.stop()
