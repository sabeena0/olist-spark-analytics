import os
import pandas as pd
from common import HERE, get_spark, load_tables
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = get_spark("LatePrediction")
t = load_tables(spark)

# ---- 1. One row per delivered order, with the label ----
orders = (t["orders"]
    .filter((F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNotNull())
    .join(t["customers"], "customer_id")
    .select("order_id", "customer_state",
            F.datediff("order_estimated_delivery_date", "order_purchase_timestamp").alias("est_days"),
            F.dayofweek("order_purchase_timestamp").alias("purchase_dow"),
            F.month("order_purchase_timestamp").alias("purchase_month"),
            (F.datediff("order_delivered_customer_date", "order_estimated_delivery_date") > 0)
                .cast("int").alias("is_late")))

# ---- 2. Order-level features from items, products and sellers ----
items = (t["items"]
    .join(t["products"].select("product_id", "product_weight_g"), "product_id", "left")
    .join(t["sellers"].select("seller_id", "seller_state"), "seller_id", "left"))

order_feats = (items.join(orders.select("order_id", "customer_state"), "order_id")
    .groupBy("order_id").agg(
        F.sum("price").alias("price"),
        F.sum("freight_value").alias("freight"),
        F.count("*").alias("n_items"),
        F.countDistinct("seller_id").alias("n_sellers"),
        F.sum(F.coalesce("product_weight_g", F.lit(0))).alias("weight_g"),
        F.max((F.col("seller_state") != F.col("customer_state")).cast("int")).alias("diff_state")))

data = (orders.join(order_feats, "order_id")
        .select("is_late", "est_days", "purchase_dow", "purchase_month",
                "price", "freight", "n_items", "n_sellers", "weight_g", "diff_state")
        .dropna())
data.cache()

n = data.count()
n_late = data.filter("is_late = 1").count()
print(f"Orders: {n}   late: {n_late} ({100 * n_late / n:.1f}%)")

# ---- 3. Class weights so late orders count more ----
w_late = n / (2 * n_late)
w_ok = n / (2 * (n - n_late))
data = data.withColumn("w", F.when(F.col("is_late") == 1, w_late).otherwise(w_ok))

features = ["est_days", "purchase_dow", "purchase_month", "price", "freight",
            "n_items", "n_sellers", "weight_g", "diff_state"]
vec = VectorAssembler(inputCols=features, outputCol="features").transform(data)
train, test = vec.randomSplit([0.8, 0.2], seed=42)

# ---- 4. Train and evaluate ----
evaluator = BinaryClassificationEvaluator(labelCol="is_late", metricName="areaUnderROC")
rows = []

def report(name, model):
    pred = model.transform(test)
    auc = evaluator.evaluate(pred)
    c = {(r["is_late"], int(r["prediction"])): r["count"]
         for r in pred.groupBy("is_late", "prediction").count().collect()}
    tp, fp = c.get((1, 1), 0), c.get((0, 1), 0)
    fn, tn = c.get((1, 0), 0), c.get((0, 0), 0)
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    print(f"\n=== {name} ===")
    print(f"AUC {auc:.3f}   precision {precision:.2f}   recall {recall:.2f}")
    print("Confusion matrix (rows = actual, columns = predicted)")
    print(f"                 pred on-time   pred late")
    print(f"actual on-time   {tn:11d}   {fp:9d}")
    print(f"actual late      {fn:11d}   {tp:9d}")
    rows.append({"model": name, "auc": round(auc, 3), "precision": round(precision, 3),
                 "recall": round(recall, 3), "tn": tn, "fp": fp, "fn": fn, "tp": tp})

lr = LogisticRegression(featuresCol="features", labelCol="is_late", weightCol="w").fit(train)
report("Logistic Regression", lr)

rf = RandomForestClassifier(featuresCol="features", labelCol="is_late", weightCol="w",
                            numTrees=50, maxDepth=8, seed=42).fit(train)
report("Random Forest", rf)

imp = (pd.DataFrame({"feature": features, "importance": rf.featureImportances.toArray()})
       .sort_values("importance", ascending=False).round(3))
print("\nFeature importance (Random Forest):")
print(imp.to_string(index=False))

# ---- 5. Save for the dashboard ----
out = os.path.join(HERE, "..", "output")
pd.DataFrame(rows).to_csv(os.path.join(out, "late_model_metrics.csv"), index=False)
imp.to_csv(os.path.join(out, "late_feature_importance.csv"), index=False)

spark.stop()