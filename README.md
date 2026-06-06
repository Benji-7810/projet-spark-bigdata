# Projet Big Data — Streaming LeBonCoin
> Plateforme de Streaming Infini d'Interactions Commerciales  
> PySpark Structured Streaming · GraphFrames · Dashboard Dynamique

---

## Architecture

```
generator/producer.py
  └─ TCP socket :9999
        │
        ▼
spark/pipeline.py  (PySpark 3.5 Structured Streaming)
  ├─ query_console : sliding window (30s/10s) + watermark → stdout
  └─ query_graph   : foreachBatch → GraphFrames → data/graph/
        │
        ▼
dashboard/app.py  (Dash + Cytoscape)
  └─ polling JSON toutes les 5s → graphe interactif sur :8050
```

---

## Prérequis

- Python 3.10+
- Java 17+ (Java 21 supporté)
- PySpark **3.5.3** (via pip — ne pas utiliser de Spark système)
- GraphFrames 0.8.4 (chargé automatiquement au démarrage)

> **Important** : si `SPARK_HOME` pointe vers une installation système Spark 4.x,  
> commenter ces lignes dans `~/.bashrc` avant de lancer :
> ```bash
> # export SPARK_HOME=...
> # export PYTHONPATH=...
> ```
> puis `source ~/.bashrc`

---

## Installation

```bash
pip install pyspark==3.5.3 graphframes dash dash-cytoscape pandas --break-system-packages
```

---

## Lancement (3 terminaux, depuis la racine du projet)

**Terminal 1 — Producteur de données :**
```bash
python3 generator/producer.py
```

**Terminal 2 — Pipeline Spark :**
```bash
python3 spark/pipeline.py
```
> Au premier lancement, Spark télécharge le JAR GraphFrames (~30s).  
> Les JSON sont réinitialisés automatiquement à chaque démarrage.

**Terminal 3 — Dashboard :**
```bash
python3 dashboard/app.py
```
Ouvrir : http://localhost:8050

---

## Structure du projet

```
├── generator/
│   └── producer.py          # Serveur TCP, génère les événements JSON
├── spark/
│   └── pipeline.py          # Pipeline Spark Structured Streaming
├── dashboard/
│   └── app.py               # Dashboard Dash + Cytoscape
├── data/
│   └── graph/
│       ├── vertices.json    # État courant des nœuds (réinitialisé au démarrage)
│       └── edges.json       # État courant des arêtes (réinitialisé au démarrage)
├── rapport_bigdata.docx     # Rapport technique
└── README.md
```

---

## Concepts PySpark implémentés

| Concept | Détail |
|---|---|
| **SparkSession** | Point d'entrée unique, `shuffle.partitions=4`, `driver.memory=2g` |
| **Schema Enforcement** | `StructType` strict — pas d'inférence automatique |
| **Structured Streaming** | Source socket TCP, micro-batches toutes les 5s |
| **Sliding Window** | `window(30s, pas 10s)` sur `action_type` |
| **Watermarking** | `withWatermark("timestamp", "10s")` — tolérance aux retards |
| **Output Mode** | `update` (console) · `append` + `foreachBatch` (graphe) |
| **GraphFrames** | `connectedComponents()` + `pageRank()` via GraphFrames 0.8.4 |
| **Deux queries parallèles** | `query_console` + `query_graph` en simultané |

---

## Modèle de Graphe (GraphFrames)

**Nœuds (Vertices)** — schéma `id · type · label · out_degree · in_degree · component_id · pagerank`

| Type | Couleur | Label affiché |
|---|---|---|
| `user` | Bleu | Ville de l'utilisateur |
| `seller` | Rouge | Identifiant vendeur |
| `product` | Vert | Catégorie du produit |

**Arêtes (Edges)** — schéma `src · dst · relationship`

| Relation | Couleur | Direction |
|---|---|---|
| `AIME` | Orange | User → Product |
| `VOUT` | Violet | User → Product |
| `ACHAT` | Rouge | User → Product |
| `PROPOSE` | Gris | Seller → Product |

> La **taille des nœuds** est proportionnelle au score **PageRank** — les nœuds les plus influents apparaissent plus grands.

---

## Dashboard

- Rafraîchissement automatique toutes les **5 secondes**
- Bouton **⏸ Pause / ▶ Reprendre** pour figer le graphe
- Barre de stats : utilisateurs · vendeurs · produits · connexions · composantes connexes · **Top 3 PageRank**
- Légende des types de nœuds et d'arêtes
