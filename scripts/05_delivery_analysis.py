from common import get_spark, load_tables

spark = get_spark("DeliveryAnalysis")
t = load_tables(spark)
for name, df in t.items():
    df.createOrReplaceTempView(name)

# Step A: one row per delivered order, with two delivery measures
spark.sql("""
    SELECT o.order_id,
           c.customer_state,
           datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
           datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS delay_days
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
""").createOrReplaceTempView("delivery")

print("=== Overall delivery performance ===")
spark.sql("""
    SELECT COUNT(*) AS delivered_orders,
           ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
           ROUND(AVG(delay_days), 1) AS avg_delay_days,
           ROUND(100 * AVG(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END), 2) AS late_pct
    FROM delivery
""").show()

print("=== Late deliveries by customer state ===")
spark.sql("""
    SELECT customer_state,
           COUNT(*) AS orders,
           ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
           ROUND(100 * AVG(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END), 2) AS late_pct
    FROM delivery
    GROUP BY customer_state
    ORDER BY late_pct DESC
""").show(30)

print("=== Review score: on-time vs late ===")
spark.sql("""
    WITH review_per_order AS (
        SELECT order_id, AVG(review_score) AS score
        FROM reviews
        GROUP BY order_id
    )
    SELECT CASE WHEN d.delay_days > 0 THEN 'late' ELSE 'on time' END AS delivery_status,
           COUNT(*) AS orders,
           ROUND(AVG(r.score), 2) AS avg_review
    FROM delivery d
    JOIN review_per_order r ON d.order_id = r.order_id
    GROUP BY delivery_status
""").show()

spark.stop()
