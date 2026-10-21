from common import get_spark, load_tables

spark = get_spark("BusinessAnalysis")
t = load_tables(spark)
for name, df in t.items():
    df.createOrReplaceTempView(name)

print("=== Overall numbers ===")
spark.sql("""
    SELECT COUNT(DISTINCT o.order_id) AS orders,
           COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
           ROUND(SUM(p.payment_value), 2) AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN payments p ON o.order_id = p.order_id
""").show()

print("=== Orders and revenue per month ===")
spark.sql("""
    SELECT date_format(o.order_purchase_timestamp, 'yyyy-MM') AS month,
           COUNT(DISTINCT o.order_id) AS orders,
           ROUND(SUM(p.payment_value), 2) AS revenue
    FROM orders o
    JOIN payments p ON o.order_id = p.order_id
    GROUP BY month
    ORDER BY month
""").show(30)

print("=== Top 10 product categories by revenue ===")
spark.sql("""
    SELECT COALESCE(c.product_category_name_english, 'unknown') AS category,
           COUNT(DISTINCT i.order_id) AS orders,
           ROUND(SUM(i.price), 2) AS revenue
    FROM items i
    JOIN products p ON i.product_id = p.product_id
    LEFT JOIN category c ON p.product_category_name = c.product_category_name
    GROUP BY category
    ORDER BY revenue DESC
    LIMIT 10
""").show(truncate=False)

spark.stop()