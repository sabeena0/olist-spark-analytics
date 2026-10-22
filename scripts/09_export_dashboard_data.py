import os
import pandas as pd
from common import HERE, get_spark, load_tables

OUT = os.path.join(HERE, "..", "output")
spark = get_spark("ExportDashboardData")
t = load_tables(spark)
for name, df in t.items():
    df.createOrReplaceTempView(name)


def save(sql, filename):
    pdf = spark.sql(sql).toPandas()
    pdf.to_csv(os.path.join(OUT, filename), index=False)
    print(f"saved {filename}: {len(pdf)} rows")


# one row per delivered order, used by several queries below
spark.sql("""
    SELECT o.order_id, c.customer_state,
           datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
           datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS delay_days
    FROM orders o JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
""").createOrReplaceTempView("delivery")

# headline numbers
overall = spark.sql("""
    SELECT COUNT(DISTINCT o.order_id) AS orders,
           COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
           ROUND(SUM(p.payment_value), 2) AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN payments p ON o.order_id = p.order_id
""").first()
avg_review = spark.sql("SELECT ROUND(AVG(review_score), 2) AS r FROM reviews").first()["r"]
deliv = spark.sql("""
    SELECT ROUND(AVG(delivery_days), 1) AS days,
           ROUND(100 * AVG(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END), 2) AS late_pct
    FROM delivery
""").first()
pd.DataFrame([{"orders": overall["orders"], "unique_customers": overall["unique_customers"],
               "revenue": overall["revenue"], "avg_review": avg_review,
               "avg_delivery_days": deliv["days"], "late_pct": deliv["late_pct"]}]
             ).to_csv(os.path.join(OUT, "kpis.csv"), index=False)
print("saved kpis.csv: 1 row")

save("""
    SELECT date_format(o.order_purchase_timestamp, 'yyyy-MM') AS month,
           COUNT(DISTINCT o.order_id) AS orders,
           ROUND(SUM(p.payment_value), 2) AS revenue
    FROM orders o JOIN payments p ON o.order_id = p.order_id
    GROUP BY month ORDER BY month
""", "monthly.csv")

save("""
    SELECT COALESCE(c.product_category_name_english, 'unknown') AS category,
           COUNT(DISTINCT i.order_id) AS orders,
           ROUND(SUM(i.price), 2) AS revenue
    FROM items i
    JOIN products p ON i.product_id = p.product_id
    LEFT JOIN category c ON p.product_category_name = c.product_category_name
    GROUP BY category ORDER BY revenue DESC LIMIT 10
""", "category_revenue.csv")

save("""
    SELECT customer_state, COUNT(*) AS orders,
           ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
           ROUND(100 * AVG(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END), 2) AS late_pct
    FROM delivery GROUP BY customer_state ORDER BY late_pct DESC
""", "delivery_by_state.csv")

save("""
    WITH review_per_order AS (
        SELECT order_id, AVG(review_score) AS score FROM reviews GROUP BY order_id
    )
    SELECT CASE WHEN d.delay_days > 0 THEN 'late' ELSE 'on time' END AS delivery_status,
           COUNT(*) AS orders, ROUND(AVG(r.score), 2) AS avg_review
    FROM delivery d JOIN review_per_order r ON d.order_id = r.order_id
    GROUP BY delivery_status
""", "review_by_delivery.csv")

spark.stop()
