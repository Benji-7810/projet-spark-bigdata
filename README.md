# Projet Big Data — Streaming LeBonCoin
> PySpark Structured Streaming + Graphe de Connexions + Dashboard Dynamique

## Architecture
producer.py ──socket TCP:9999──▶ pipeline.py (Spark) ──JSON──▶ dashboard/app.py


## Prérequis
- Python 3.10+
- Java 21+
- PySpark 4.1.2
- pandas >= 2.2.0
- dash, dash-cytoscape

## Installation
```bash
pip install pyspark dash dash-cytoscape pandas --break-system-packages
```

## Lancement (3 terminaux)

**Terminal 1 — Producteur de données :**
```bash
python3 generator/producer.py
```

**Terminal 2 — Pipeline Spark :**
```bash
python3 spark/pipeline.py
```

**Terminal 3 — Dashboard :**
```bash
python3 dashboard/app.py
```
Ouvrir : http://localhost:8050

## Structure du projet

projet-spark-bigdata/
├── generator/
│   └── producer.py      # Simulateur de flux JSON infini
├── spark/
│   └── pipeline.py      # PySpark Structured Streaming + Graphe
├── dashboard/
│   └── app.py           # Dashboard Dash + Cytoscape
├── data/
│   └── graph/           # Fichiers JSON générés par Spark
│       ├── vertices.json
│       └── edges.json
└── README.md



## Concepts PySpark implémentés
- SparkSession avec configuration optimisée
- Schema Enforcement (schéma strict)
- Structured Streaming (socket TCP)
- Sliding Window (30s, avance 10s)
- Watermarking (tolérance 10s de retard)
- Output Mode "update"
- foreachBatch pour construction du graphe
- Calcul des degrés (centralité des nœuds)

## Modèle de Graphe
- **Nœuds** : Utilisateurs (bleu), Vendeurs (rouge), Produits (vert)
- **Arêtes** : AIME, VOUT, ACHAT (User→Product), PROPOSE (Seller→Product)
