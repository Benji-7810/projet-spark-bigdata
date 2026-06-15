from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from graphframes import GraphFrame
import json
import os

# ─────────────────────────────────────────
# 1. SparkSession  (GraphFrames via spark.jars.packages)
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LeBonCoin-Streaming") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

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
# 5. État global du graphe
# ─────────────────────────────────────────
VERTICES_PATH = "data/graph/vertices.json"
EDGES_PATH    = "data/graph/edges.json"

os.makedirs("data/graph", exist_ok=True)
with open(VERTICES_PATH, "w") as f:
    json.dump([], f)
with open(EDGES_PATH, "w") as f:
    json.dump([], f)

def load_state():
    vertices = {}
    edges    = set()

    if os.path.exists(VERTICES_PATH) and os.path.getsize(VERTICES_PATH) > 0:
        with open(VERTICES_PATH) as f:
            for v in json.load(f):
                vertices[v["id"]] = v

    if os.path.exists(EDGES_PATH) and os.path.getsize(EDGES_PATH) > 0:
        with open(EDGES_PATH) as f:
            for e in json.load(f):
                edges.add((e["src"], e["dst"], e["relationship"]))

    return vertices, edges

def save_state(vertices: dict, edges: set):
    with open(VERTICES_PATH, "w") as f:
        json.dump(list(vertices.values()), f, indent=2)

    with open(EDGES_PATH, "w") as f:
        json.dump([
            {"src": s, "dst": d, "relationship": r}
            for s, d, r in edges
        ], f, indent=2)

def build_graph(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    vertices, edges = load_state()

    for row in rows:
        if row.user_id not in vertices:
            vertices[row.user_id] = {
                "id": row.user_id, "type": "user",
                "label": row.user_city,
                "out_degree": 0, "in_degree": 0,
                "pagerank": 0.0, "component_id": None
            }
        if row.seller_id not in vertices:
            vertices[row.seller_id] = {
                "id": row.seller_id, "type": "seller",
                "label": row.seller_id,
                "out_degree": 0, "in_degree": 0,
                "pagerank": 0.0, "component_id": None
            }
        if row.product_id not in vertices:
            vertices[row.product_id] = {
                "id": row.product_id, "type": "product",
                "label": row.product_cat,
                "out_degree": 0, "in_degree": 0,
                "pagerank": 0.0, "component_id": None
            }

        edge_up = (row.user_id, row.product_id, row.action_type)
        if edge_up not in edges:
            edges.add(edge_up)
            vertices[row.user_id]["out_degree"]   += 1
            vertices[row.product_id]["in_degree"] += 1

        edge_sp = (row.seller_id, row.product_id, "PROPOSE")
        if edge_sp not in edges:
            edges.add(edge_sp)
            vertices[row.seller_id]["out_degree"]  += 1
            vertices[row.product_id]["in_degree"]  += 1

    # ── Composantes connexes via GraphFrames ──
    v_df = spark.createDataFrame(
        [(v["id"], v["type"], v.get("label", "")) for v in vertices.values()],
        ["id", "type", "label"]
    )
    e_df = spark.createDataFrame(
        [(s, d, r) for s, d, r in edges],
        ["src", "dst", "relationship"]
    )

    g = GraphFrame(v_df, e_df)

    # Composantes connexes
    components = g.connectedComponents()
    for row in components.collect():
        if row["id"] in vertices:
            vertices[row["id"]]["component_id"] = str(row["component"])

    nb_components = len(set(
        v.get("component_id", v["id"]) for v in vertices.values()
    ))

    # PageRank
    pr = g.pageRank(resetProbability=0.15, maxIter=3)
    for row in pr.vertices.collect():
        if row["id"] in vertices:
            vertices[row["id"]]["pagerank"] = round(float(row["pagerank"]), 3)

    top_pr = sorted(vertices.values(), key=lambda v: v.get("pagerank", 0), reverse=True)[:3]

    save_state(vertices, edges)

    print(f"[BATCH {batch_id}] "
          f"Vertices: {len(vertices)} | "
          f"Edges: {len(edges)} | "
          f"Composantes: {nb_components} | "
          f"Top PageRank: {[(v['id'], v.get('pagerank',0)) for v in top_pr]}")

# ─────────────────────────────────────────
# 6. Deux queries en parallèle
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

spark.streams.awaitAnyTermination()
