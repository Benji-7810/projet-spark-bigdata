from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from graphframes import GraphFrame
import json
import os

# ─────────────────────────────────────────
# 1. SPARKSESSION — Point d'entrée Spark
#
# C'est le "Driver" : il crée la session, configure le cluster,
# construit le DAG et distribue les tâches aux Executors.
# ─────────────────────────────────────────
# télécharge GraphFrames au 1er lancement (~30s)
# 4 partitions = 4 tâches = 4 cœurs max
# désactive l'optimisation adaptative pour plus de stabilité
spark = SparkSession.builder \
    .appName("LeBonCoin-Streaming") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # réduit le bruit dans les logs

# Dossier pour sauvegarder l'état des fenêtres glissantes entre micro-batches
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")


# ─────────────────────────────────────────
# 2. SCHÉMA STRICT (StructType)
#
# On déclare les types exacts attendus dans le JSON entrant.
# Avantage : Spark n'a pas besoin d'inférer le schéma → plus rapide.
# C'est l'approche DataFrame (déclarative) vs RDD (impérative).
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
# 3. SOURCE SOCKET — Lecture du flux TCP
#
# Spark se connecte au producer sur localhost:9999.
# Chaque ligne reçue devient une ligne du DataFrame "raw_stream".
# ─────────────────────────────────────────
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Transformation NARROW (pas de shuffle) :
# on parse la colonne "value" (string brut) en colonnes structurées
df_parsed = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")


# ─────────────────────────────────────────
# 4. FENÊTRE GLISSANTE + WATERMARK
#
# Watermark : tolère jusqu'à 10s de retard sur les événements.
# Fenêtre glissante : regroupe les événements sur 30s, avance de 10s en 10s.
# → chaque événement appartient à plusieurs fenêtres simultanément.
#
# C'est une transformation WIDE (déclenche un shuffle pour agréger
# les comptages depuis plusieurs partitions).
# ─────────────────────────────────────────
df_windowed = df_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("action_type")
    ).count()


# ─────────────────────────────────────────
# 5. ÉTAT GLOBAL DU GRAPHE (fichiers JSON partagés)
#
# Le graphe est persisté sur disque dans data/graph/.
# Le dashboard Dash lit ces mêmes fichiers toutes les 5s.
# Pas de base de données ni de broker — le fichier JSON EST la mémoire partagée.
# ─────────────────────────────────────────
VERTICES_PATH = "data/graph/vertices.json"
EDGES_PATH    = "data/graph/edges.json"

# Réinitialisation au démarrage : repart d'un graphe vide à chaque lancement
os.makedirs("data/graph", exist_ok=True)
with open(VERTICES_PATH, "w") as f:
    json.dump([], f)
with open(EDGES_PATH, "w") as f:
    json.dump([], f)


def load_state():
    """Charge les nœuds et arêtes existants depuis les fichiers JSON."""
    vertices = {}  # dict id → nœud
    edges    = set()  # set de tuples (src, dst, relationship) — évite les doublons

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
    """Sauvegarde l'état courant du graphe dans les fichiers JSON."""
    with open(VERTICES_PATH, "w") as f:
        json.dump(list(vertices.values()), f, indent=2)

    with open(EDGES_PATH, "w") as f:
        json.dump([
            {"src": s, "dst": d, "relationship": r}
            for s, d, r in edges
        ], f, indent=2)


def build_graph(batch_df, batch_id):
    """
    Fonction appelée par foreachBatch à chaque micro-batch (toutes les 5s).

    foreachBatch donne accès au micro-batch comme un DataFrame STATIQUE,
    ce qui permet d'utiliser du code Python/Pandas arbitraire.
    Cette fonction :
      1. Charge l'état courant du graphe
      2. Ajoute les nouveaux nœuds et arêtes du batch
      3. Calcule les composantes connexes et le PageRank via GraphFrames
      4. Sauvegarde le nouvel état
    """
    rows = batch_df.collect()  # ramène les lignes du batch sur le Driver
    if not rows:
        return  # batch vide → rien à faire

    vertices, edges = load_state()

    # ── Ajout des nœuds et arêtes du batch ──
    for row in rows:
        # Créer les nœuds s'ils n'existent pas encore
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

        # Arête utilisateur → produit (AIME / VOUT / ACHAT)
        # Le set évite les doublons : une même arête n'est ajoutée qu'une fois
        edge_up = (row.user_id, row.product_id, row.action_type)
        if edge_up not in edges:
            edges.add(edge_up)
            vertices[row.user_id]["out_degree"]   += 1
            vertices[row.product_id]["in_degree"] += 1

        # Arête vendeur → produit (PROPOSE)
        edge_sp = (row.seller_id, row.product_id, "PROPOSE")
        if edge_sp not in edges:
            edges.add(edge_sp)
            vertices[row.seller_id]["out_degree"]  += 1
            vertices[row.product_id]["in_degree"]  += 1

    # ── GraphFrames : analyse du graphe complet ──
    # On recrée un GraphFrame à chaque batch avec l'état cumulé
    v_df = spark.createDataFrame(
        [(v["id"], v["type"], v.get("label", "")) for v in vertices.values()],
        ["id", "type", "label"]
    )
    e_df = spark.createDataFrame(
        [(s, d, r) for s, d, r in edges],
        ["src", "dst", "relationship"]
    )
    g = GraphFrame(v_df, e_df)

    # Composantes connexes : identifie les sous-graphes déconnectés
    # (ex: un vendeur sans lien avec les utilisateurs → composante séparée)
    components = g.connectedComponents()
    for row in components.collect():
        if row["id"] in vertices:
            vertices[row["id"]]["component_id"] = str(row["component"])

    nb_components = len(set(
        v.get("component_id", v["id"]) for v in vertices.values()
    ))

    # PageRank : mesure l'influence de chaque nœud dans le graphe
    # resetProbability=0.15 → facteur d'amortissement classique (Google = 0.15)
    # maxIter=3 → 3 itérations suffisent pour un petit graphe
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
# 6. DEUX QUERIES EN PARALLÈLE
#
# Spark lance deux flux simultanément depuis le même DataFrame source.
# Chaque query tourne indépendamment et se déclenche toutes les 5s.
# ─────────────────────────────────────────

# Query 1 : affiche les comptages par action dans la console
# outputMode "update" → n'écrit que les lignes qui ont changé depuis le dernier trigger
query_console = df_windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# Query 2 : construit et met à jour le graphe via foreachBatch
# outputMode "append" → chaque ligne du batch est traitée une seule fois
query_graph = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(build_graph) \
    .trigger(processingTime="5 seconds") \
    .start()

# Attend que les deux queries se terminent (tourne indéfiniment jusqu'à Ctrl+C)
query_console.awaitTermination()
query_graph.awaitTermination()
