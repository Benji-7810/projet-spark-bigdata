import json
import os
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_cytoscape as cyto

cyto.load_extra_layouts()

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("LeBonCoin — Graphe de Connexions",
            style={"textAlign": "center", "fontFamily": "Arial"}),

    # Légende
    html.Div([
        html.Span("● Utilisateur", style={"color": "#4A90D9", "marginRight": "20px"}),
        html.Span("● Vendeur",     style={"color": "#E74C3C", "marginRight": "20px"}),
        html.Span("● Produit",     style={"color": "#2ECC71", "marginRight": "30px"}),
        html.Span("— AIME",    style={"color": "#F39C12", "marginRight": "15px"}),
        html.Span("— VOUT",    style={"color": "#9B59B6", "marginRight": "15px"}),
        html.Span("— ACHAT",   style={"color": "#E74C3C", "marginRight": "15px"}),
        html.Span("— PROPOSE", style={"color": "#95A5A6"}),
    ], style={"textAlign": "center", "fontSize": "13px", "marginBottom": "10px"}),

    # Bouton pause
    html.Div([
        html.Button("⏸ Pause", id="btn-pause", n_clicks=0, style={
            "padding": "8px 20px",
            "fontSize": "14px",
            "cursor": "pointer",
            "borderRadius": "6px",
            "border": "none",
            "backgroundColor": "#E74C3C",
            "color": "white",
            "fontWeight": "bold",
        }),
    ], style={"textAlign": "center", "marginBottom": "10px"}),

    # Graphe Cytoscape
    cyto.Cytoscape(
        id="graph",
        layout={"name": "cose"},
        style={"width": "100%", "height": "600px", "border": "1px solid #ccc"},
        elements=[],
        stylesheet=[
            {
                "selector": "node",
                "style": {
                    "label": "data(label)",
                    "font-size": "7px",
                    "text-valign": "center",
                    "color": "white",
                    "text-outline-width": 1,
                    "text-outline-color": "#333",
                    "width": "data(size)",
                    "height": "data(size)",
                }
            },
            {"selector": ".user",    "style": {"background-color": "#4A90D9"}},
            {"selector": ".seller",  "style": {"background-color": "#E74C3C"}},
            {"selector": ".product", "style": {"background-color": "#2ECC71"}},
            {
                "selector": "edge",
                "style": {
                    "curve-style": "bezier",
                    "target-arrow-shape": "triangle",
                    "arrow-scale": 1.5,
                    "line-color": "#aaa",
                    "target-arrow-color": "#aaa",
                }
            },
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
    dcc.Interval(id="interval", interval=5000, n_intervals=0, disabled=False),
])

# ─────────────────────────────────────────
# Callback : bouton pause/reprendre
# ─────────────────────────────────────────
@app.callback(
    Output("interval", "disabled"),
    Output("btn-pause", "children"),
    Output("btn-pause", "style"),
    Input("btn-pause", "n_clicks"),
    State("interval", "disabled"),
)
def toggle_pause(n_clicks, is_disabled):
    if n_clicks == 0:
        return False, "⏸ Pause", _btn_style("#E74C3C")
    if is_disabled:
        return False, "⏸ Pause", _btn_style("#E74C3C")
    else:
        return True, "▶ Reprendre", _btn_style("#2ECC71")

def _btn_style(color):
    return {
        "padding": "8px 20px",
        "fontSize": "14px",
        "cursor": "pointer",
        "borderRadius": "6px",
        "border": "none",
        "backgroundColor": color,
        "color": "white",
        "fontWeight": "bold",
    }

# ─────────────────────────────────────────
# Callback : relit les JSON et met à jour le graphe
# ─────────────────────────────────────────
@app.callback(
    Output("graph", "elements"),
    Output("stats", "children"),
    Input("interval", "n_intervals")
)
def update_graph(_):
    vertices_path = "data/graph/vertices.json"
    edges_path    = "data/graph/edges.json"

    if not os.path.exists(vertices_path) or not os.path.exists(edges_path):
        return [], "En attente des données Spark..."

    with open(vertices_path) as f:
        vertices = json.load(f)
    with open(edges_path) as f:
        edges = json.load(f)

    elements = []

    for v in vertices:
        pagerank = v.get("pagerank", 0)
        size     = max(20, min(70, 20 + pagerank * 30))
        elements.append({
            "data": {
                "id":           v["id"],
                "label":        v.get("label", v["id"]),
                "type":         v["type"],
                "size":         size,
                "pagerank":     pagerank,
                "relationship": "",
            },
            "classes": v["type"]
        })

    for e in edges:
        elements.append({
            "data": {
                "id":           f"{e['src']}__{e['dst']}__{e['relationship']}",
                "source":       e["src"],
                "target":       e["dst"],
                "relationship": e["relationship"],
            },
            "classes": e["relationship"]
        })

    components    = set(v.get("component_id", v["id"]) for v in vertices)
    nb_components = len(components)

    top_pr = sorted(vertices, key=lambda v: v.get("pagerank", 0), reverse=True)[:3]
    top_str = " | ".join(
        f"{v.get('label', v['id'])} ({v.get('pagerank', 0):.2f})"
        for v in top_pr
    )

    stats = f"🔵 {sum(1 for v in vertices if v['type']=='user')} utilisateurs | "\
            f"🔴 {sum(1 for v in vertices if v['type']=='seller')} vendeurs | "\
            f"🟢 {sum(1 for v in vertices if v['type']=='product')} produits | "\
            f"🔗 {len(edges)} connexions | "\
            f"🔀 {nb_components} composantes | "\
            f"⭐ Top PageRank : {top_str}"

    return elements, stats

if __name__ == "__main__":
    app.run(debug=True, port=8050)
