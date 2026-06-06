from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import json
import os

# ─────────────────────────────────────────
# Union-Find — équivalent connectedComponents()
# ─────────────────────────────────────────
class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[rx] = ry

    def get_components(self, nodes):
        return {node: self.find(node) for node in nodes}

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
    os.makedirs("data/graph", exist_ok=True)

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
        # Vertices
        if row.user_id not in vertices:
            vertices[row.user_id] = {
                "id": row.user_id, "type": "user",
                "label": row.user_city,
                "out_degree": 0, "in_degree": 0
            }
        if row.seller_id not in vertices:
            vertices[row.seller_id] = {
                "id": row.seller_id, "type": "seller",
                "label": row.seller_id,
                "out_degree": 0, "in_degree": 0
            }
        if row.product_id not in vertices:
            vertices[row.product_id] = {
                "id": row.product_id, "type": "product",
                "label": row.product_cat,
                "out_degree": 0, "in_degree": 0
            }

        # Edges
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

    # ── Composantes connexes ──
    uf = UnionFind()
    for src, dst, _ in edges:
        uf.union(src, dst)

    components  = uf.get_components(list(vertices.keys()))
    nb_components = len(set(components.values()))

    for node_id, comp_id in components.items():
        if node_id in vertices:
            vertices[node_id]["component_id"] = comp_id

    save_state(vertices, edges)

    print(f"[BATCH {batch_id}] "
          f"Vertices: {len(vertices)} | "
          f"Edges: {len(edges)} | "
          f"Composantes connexes: {nb_components}")

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

query_console.awaitTermination()
query_graph.awaitTermination()