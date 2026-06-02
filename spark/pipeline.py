from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import os

# ─────────────────────────────────────────
# 1. SparkSession
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LeBonCoin-Streaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# 2. Schéma strict
# ─────────────────────────────────────────
schema = StructType([
    StructField("timestamp",   TimestampType(), True),
    StructField("user_id",     StringType(),    True),
    StructField("user_city",   StringType(),    True),
    StructField("product_id",  StringType(),    True),
    StructField("product_cat", StringType(),    True),
    StructField("seller_id",   StringType(),    True),
    StructField("action_type", StringType(),    True),
    StructField("price",       DoubleType(),    True),
])

# ─────────────────────────────────────────
# 3. Lecture flux socket
# ─────────────────────────────────────────
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

df_parsed = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ─────────────────────────────────────────
# 4. Watermark + Fenêtre glissante
# ─────────────────────────────────────────
df_windowed = df_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("action_type")
    ).count()

# ─────────────────────────────────────────
# 5. Construction du graphe
#    Vertices : id unique + type + label
#    Edges    : src -> dst + type relation
# ─────────────────────────────────────────
def build_graph(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # --- VERTICES ---
    # Utilisateurs
    users = batch_df.select(
        col("user_id").alias("id"),
        lit("user").alias("type"),
        col("user_city").alias("label")
    )
    # Vendeurs
    sellers = batch_df.select(
        col("seller_id").alias("id"),
        lit("seller").alias("type"),
        col("seller_id").alias("label")
    )
    # Produits
    products = batch_df.select(
        col("product_id").alias("id"),
        lit("product").alias("type"),
        col("product_cat").alias("label")
    )

    vertices = users.union(sellers).union(products).distinct()

    # --- EDGES ---
    # User → Product  (action directe)
    edges_user_product = batch_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship"),
        col("price")
    )
    # Seller → Product  (propose)
    edges_seller_product = batch_df.select(
        col("seller_id").alias("src"),
        col("product_id").alias("dst"),
        lit("PROPOSE").alias("relationship"),
        col("price")
    )

    edges = edges_user_product.union(
        edges_seller_product.drop("price").withColumn("price", lit(0.0))
    ).distinct()

    # --- Calcul des degrés (centralité) ---
    # Degré = nombre de connexions d'un nœud
    out_degrees = edges.groupBy("src").count().withColumnRenamed("count", "out_degree")
    in_degrees  = edges.groupBy("dst").count().withColumnRenamed("count", "in_degree")

    vertices_with_degrees = vertices \
        .join(out_degrees, vertices.id == out_degrees.src, "left") \
        .join(in_degrees,  vertices.id == in_degrees.dst,  "left") \
        .drop("src", "dst") \
        .fillna(0)

    # --- Sauvegarde pour le dashboard ---
    os.makedirs("data/graph", exist_ok=True)

    vertices_with_degrees.toPandas().to_json(
        "data/graph/vertices.json", orient="records", indent=2
    )
    edges.toPandas().to_json(
        "data/graph/edges.json", orient="records", indent=2
    )

    print(f"[BATCH {batch_id}] Vertices: {vertices_with_degrees.count()} | Edges: {edges.count()}")

# ─────────────────────────────────────────
# 6. Deux queries en parallèle :
#    - console : affiche les stats fenêtrées
#    - foreachBatch : construit le graphe
# ─────────────────────────────────────────
query_console = df_windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query_graph = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(build_graph) \
    .trigger(processingTime="5 seconds") \
    .start()

query_console.awaitTermination()
query_graph.awaitTermination()