import json
import os
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_cytoscape as cyto

# Charger le layout Cytoscape
cyto.load_extra_layouts()

app = dash.Dash(__name__)

# ─────────────────────────────────────────
# Couleurs par type de nœud
# ─────────────────────────────────────────
COLORS = {
    "user":    "#4A90D9",  # bleu
    "seller":  "#E74C3C",  # rouge
    "product": "#2ECC71",  # vert
}

app.layout = html.Div([
    html.H1("LeBonCoin — Graphe de Connexions",
            style={"textAlign": "center", "fontFamily": "Arial"}),

    # Légende
    html.Div([
        html.Span("● Utilisateur", style={"color": "#4A90D9", "marginRight": "20px"}),
        html.Span("● Vendeur",     style={"color": "#E74C3C", "marginRight": "20px"}),
        html.Span("● Produit",     style={"color": "#2ECC71"}),
    ], style={"textAlign": "center", "fontSize": "16px", "marginBottom": "10px"}),

    # Graphe Cytoscape
    cyto.Cytoscape(
        id="graph",
        layout={"name": "cose"},  # layout automatique
        style={"width": "100%", "height": "600px", "border": "1px solid #ccc"},
        elements=[],
        stylesheet=[
            # Style des nœuds
            {
                "selector": "node",
                "style": {
                    "label": "data(label)",
                    "font-size": "10px",
                    "text-valign": "center",
                    "color": "white",
                    "text-outline-width": 2,
                    "text-outline-color": "#555",
                    "width": "data(size)",
                    "height": "data(size)",
                }
            },
            # Couleur par type
            {"selector": ".user",    "style": {"background-color": "#4A90D9"}},
            {"selector": ".seller",  "style": {"background-color": "#E74C3C"}},
            {"selector": ".product", "style": {"background-color": "#2ECC71"}},
            # Style des arêtes
            {
                "selector": "edge",
                "style": {
                    "label": "data(relationship)",
                    "font-size": "8px",
                    "curve-style": "bezier",
                    "target-arrow-shape": "triangle",
                    "arrow-scale": 1.5,
                    "line-color": "#aaa",
                    "target-arrow-color": "#aaa",
                }
            },
            # Couleur arête par type
            {"selector": ".AIME",    "style": {"line-color": "#F39C12", "target-arrow-color": "#F39C12"}},
            {"selector": ".VOUT",    "style": {"line-color": "#9B59B6", "target-arrow-color": "#9B59B6"}},
            {"selector": ".ACHAT",   "style": {"line-color": "#E74C3C", "target-arrow-color": "#E74C3C"}},
            {"selector": ".PROPOSE", "style": {"line-color": "#95A5A6", "target-arrow-color": "#95A5A6"}},
        ]
    ),

    # Stats en bas
    html.Div(id="stats",
             style={"textAlign": "center", "marginTop": "10px", "fontFamily": "Arial"}),

    # Rafraîchissement toutes les 5 secondes
    dcc.Interval(id="interval", interval=5000, n_intervals=0),
])

# ─────────────────────────────────────────
# Callback : relit les JSON et met à jour le graphe
# ─────────────────────────────────────────
@app.callback(
    Output("graph", "elements"),
    Output("stats", "children"),
    Input("interval", "n_intervals")
)
def update_graph(n):
    vertices_path = "data/graph/vertices.json"
    edges_path    = "data/graph/edges.json"

    if not os.path.exists(vertices_path) or not os.path.exists(edges_path):
        return [], "En attente des données Spark..."

    with open(vertices_path) as f:
        vertices = json.load(f)
    with open(edges_path) as f:
        edges = json.load(f)

    elements = []

    # Nœuds
    for v in vertices:
        degree = v.get("out_degree", 0) + v.get("in_degree", 0)
        size   = max(20, min(60, 15 + degree * 3))  # taille proportionnelle au degré
        elements.append({
            "data": {
                "id":           v["id"],
                "label":        v["id"],
                "type":         v["type"],
                "size":         size,
                "relationship": "",
            },
            "classes": v["type"]
        })

    # Arêtes
    for e in edges:
        elements.append({
            "data": {
                "source":       e["src"],
                "target":       e["dst"],
                "relationship": e["relationship"],
            },
            "classes": e["relationship"]
        })

    stats = f"🔵 {sum(1 for v in vertices if v['type']=='user')} utilisateurs | "\
            f"🔴 {sum(1 for v in vertices if v['type']=='seller')} vendeurs | "\
            f"🟢 {sum(1 for v in vertices if v['type']=='product')} produits | "\
            f"🔗 {len(edges)} connexions"

    return elements, stats

if __name__ == "__main__":
    app.run(debug=True, port=8050)