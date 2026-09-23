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
  ├─ query_console : sliding window (30s/10s) + watermark (10s) → stdout
  └─ query_graph   : foreachBatch → GraphFrames (connectedComponents + pageRank) → data/graph/
        │
        ▼
dashboard/app.py  (Dash + Cytoscape)
  └─ polling JSON toutes les 5s → graphe interactif sur :8050
```

---

## Prérequis

- Python 3.10+
- Java 17 ou Java 21
- PySpark **3.5.3** (via pip — ne pas utiliser de Spark système)
- GraphFrames 0.8.4 (chargé automatiquement via `spark.jars.packages`)

> **Important** : si `SPARK_HOME` pointe vers une installation système Spark 4.x,  
> commenter ces lignes dans `~/.bashrc` avant de lancer :
> ```bash
> # export SPARK_HOME=...
> # export PYTHONPATH=...
> ```
> puis `source ~/.bashrc` et vérifier avec `echo $SPARK_HOME` (doit être vide).

---

## Installation

```bash
pip install pyspark==3.5.3 graphframes dash dash-cytoscape pandas --break-system-packages
```

---

## Lancement (3 terminaux, depuis la racine du projet)

> **Ordre obligatoire** : le producer doit être démarré **avant** le pipeline.  
> Le producer bloque jusqu'à ce que Spark se connecte sur le port 9999.

**Terminal 1 — Producteur de données :**
```bash
python3 generator/producer.py
```

**Terminal 2 — Pipeline Spark :**
```bash
python3 spark/pipeline.py
```
> Au premier lancement, Spark télécharge le JAR GraphFrames (~30s).  
> Les fichiers JSON sont réinitialisés automatiquement à chaque démarrage.

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
│   └── pipeline.py          # Pipeline Spark Structured Streaming + GraphFrames
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
| **Schema Enforcement** | `StructType` strict — évite l'inférence automatique coûteuse |
| **Structured Streaming** | Source socket TCP, micro-batches déclenchés toutes les 5s |
| **Sliding Window** | `window("30s", pas "10s")` — agrégation de `action_type` par tranche |
| **Watermarking** | `withWatermark("timestamp", "10s")` — tolère les retards, libère la mémoire |
| **Output Mode** | `update` pour la console · `append` + `foreachBatch` pour le graphe |
| **GraphFrames** | `connectedComponents()` + `pageRank(resetProbability=0.15, maxIter=3)` |
| **Deux queries parallèles** | `query_console` + `query_graph` démarrent simultanément |
| **foreachBatch** | Accès direct au micro-batch en tant que DataFrame statique |

---

## Modèle de Graphe (GraphFrames)

**Nœuds (Vertices)** — schéma : `id · type · label · out_degree · in_degree · component_id · pagerank`

| Type | Couleur | Label affiché |
|---|---|---|
| `user` | Bleu `#4A90D9` | Ville de l'utilisateur |
| `seller` | Rouge `#E74C3C` | Identifiant vendeur |
| `product` | Vert `#2ECC71` | Catégorie du produit |

**Arêtes (Edges)** — schéma : `src · dst · relationship`

| Relation | Couleur | Direction |
|---|---|---|
| `AIME` | Orange | User → Product |
| `VOUT` | Violet | User → Product |
| `ACHAT` | Rouge | User → Product |
| `PROPOSE` | Gris | Seller → Product |

> La **taille des nœuds** est proportionnelle au score **PageRank** — les nœuds les plus influents (produits très convoités, vendeurs très actifs) apparaissent visuellement plus grands.

---

## Dashboard

- Rafraîchissement automatique toutes les **5 secondes**
- Bouton **⏸ Pause / ▶ Reprendre** pour figer le graphe à n'importe quel moment
- Barre de stats en temps réel : utilisateurs · vendeurs · produits · connexions · composantes connexes · **Top 3 PageRank**
- Légende des types de nœuds et des types d'arêtes par couleur

---

## Dépannage

| Problème | Cause | Solution |
|---|---|---|
| `ClassNotFoundException: scala.Serializable` | Spark système 4.x actif | Commenter `SPARK_HOME` dans `~/.bashrc` |
| `Connection refused` au démarrage | Producer pas encore lancé | Démarrer le producer **avant** le pipeline |
| Graphe vide dans le dashboard | Fichiers JSON corrompus | Les JSON se réinitialisent automatiquement au redémarrage du pipeline |
| `OutOfMemoryError` avec GraphFrames | Mémoire driver insuffisante | Déjà configuré : `spark.driver.memory=2g` dans pipeline.py |
