import json
import os
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_cytoscape as cyto

# Active les layouts supplémentaires de Cytoscape (ex: "cose", "dagre"...)
cyto.load_extra_layouts()

app = dash.Dash(__name__)

# ─────────────────────────────────────────
# MISE EN PAGE (LAYOUT)
#
# Dash utilise des composants Python pour décrire le HTML.
# La page est composée de :
#   - Un titre
#   - Une légende des couleurs
#   - Un bouton Pause / Reprendre
#   - Le graphe interactif (Cytoscape)
#   - Une barre de stats
#   - Un timer invisible qui se déclenche toutes les 5s
# ─────────────────────────────────────────
app.layout = html.Div([
    html.H1("LeBonCoin — Graphe de Connexions",
            style={"textAlign": "center", "fontFamily": "Arial"}),

    # Légende : couleurs des nœuds et des arêtes
    html.Div([
        html.Span("● Utilisateur", style={"color": "#4A90D9", "marginRight": "20px"}),
        html.Span("● Vendeur",     style={"color": "#E74C3C", "marginRight": "20px"}),
        html.Span("● Produit",     style={"color": "#2ECC71", "marginRight": "30px"}),
        html.Span("— AIME",    style={"color": "#F39C12", "marginRight": "15px"}),
        html.Span("— VOUT",    style={"color": "#9B59B6", "marginRight": "15px"}),
        html.Span("— ACHAT",   style={"color": "#E74C3C", "marginRight": "15px"}),
        html.Span("— PROPOSE", style={"color": "#95A5A6"}),
    ], style={"textAlign": "center", "fontSize": "13px", "marginBottom": "10px"}),

    # Bouton pour figer ou reprendre le rafraîchissement du graphe
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

    # ── Composant principal : graphe interactif ──
    # "elements" est mis à jour dynamiquement par le callback update_graph
    # "stylesheet" définit les règles visuelles (couleur, taille, forme des flèches)
    cyto.Cytoscape(
        id="graph",
        layout={"name": "cose"},  # layout "cose" = physique de ressorts, bien pour les graphes
        style={"width": "100%", "height": "600px", "border": "1px solid #ccc"},
        elements=[],  # vide au démarrage, rempli par le callback
        stylesheet=[
            # Style par défaut pour tous les nœuds
            {
                "selector": "node",
                "style": {
                    "label": "data(label)",       # affiche le label stocké dans les données
                    "font-size": "7px",
                    "text-valign": "center",
                    "color": "white",
                    "text-outline-width": 1,
                    "text-outline-color": "#333",
                    "width": "data(size)",        # taille proportionnelle au PageRank
                    "height": "data(size)",
                }
            },
            # Couleur selon le type de nœud (classe CSS assignée dans le callback)
            {"selector": ".user",    "style": {"background-color": "#4A90D9"}},
            {"selector": ".seller",  "style": {"background-color": "#E74C3C"}},
            {"selector": ".product", "style": {"background-color": "#2ECC71"}},
            # Style par défaut pour toutes les arêtes
            {
                "selector": "edge",
                "style": {
                    "curve-style": "bezier",           # courbe élégante
                    "target-arrow-shape": "triangle",  # flèche directionnelle
                    "arrow-scale": 1.5,
                    "line-color": "#aaa",
                    "target-arrow-color": "#aaa",
                }
            },
            # Couleur selon le type de relation (classe CSS assignée dans le callback)
            {"selector": ".AIME",    "style": {"line-color": "#F39C12", "target-arrow-color": "#F39C12"}},
            {"selector": ".VOUT",    "style": {"line-color": "#9B59B6", "target-arrow-color": "#9B59B6"}},
            {"selector": ".ACHAT",   "style": {"line-color": "#E74C3C", "target-arrow-color": "#E74C3C"}},
            {"selector": ".PROPOSE", "style": {"line-color": "#95A5A6", "target-arrow-color": "#95A5A6"}},
        ]
    ),

    # Zone d'affichage des statistiques (mise à jour par le callback)
    html.Div(id="stats",
             style={"textAlign": "center", "marginTop": "10px", "fontFamily": "Arial"}),

    # Timer invisible : se déclenche toutes les 5000ms (5s)
    # Quand disabled=True (pause), il ne se déclenche plus
    dcc.Interval(id="interval", interval=5000, n_intervals=0, disabled=False),
])


# ─────────────────────────────────────────
# CALLBACK 1 : Bouton Pause / Reprendre
#
# Un callback Dash s'exécute côté serveur dès qu'un Input change.
# Ici : quand le bouton est cliqué, on inverse l'état du timer.
# ─────────────────────────────────────────
@app.callback(
    Output("interval", "disabled"),    # active ou désactive le timer
    Output("btn-pause", "children"),   # change le texte du bouton
    Output("btn-pause", "style"),      # change la couleur du bouton
    Input("btn-pause", "n_clicks"),    # déclenché à chaque clic
    State("interval", "disabled"),     # lit l'état actuel du timer (sans le déclencher)
)
def toggle_pause(n_clicks, is_disabled):
    if n_clicks == 0:
        # Premier rendu : timer actif, bouton rouge "Pause"
        return False, "⏸ Pause", _btn_style("#E74C3C")
    if is_disabled:
        # Timer était en pause → on le relance
        return False, "⏸ Pause", _btn_style("#E74C3C")
    else:
        # Timer était actif → on le met en pause
        return True, "▶ Reprendre", _btn_style("#2ECC71")


def _btn_style(color):
    """Retourne le style du bouton avec la couleur donnée."""
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
# CALLBACK 2 : Mise à jour du graphe
#
# Déclenché toutes les 5s par le timer (dcc.Interval).
# Lit les fichiers JSON produits par pipeline.py et reconstruit
# la liste d'éléments Cytoscape (nœuds + arêtes).
# ─────────────────────────────────────────
@app.callback(
    Output("graph", "elements"),   # met à jour les éléments du graphe
    Output("stats", "children"),   # met à jour la barre de stats
    Input("interval", "n_intervals")  # se déclenche à chaque tick du timer
)
def update_graph(_):
    vertices_path = "data/graph/vertices.json"
    edges_path    = "data/graph/edges.json"

    # Si les fichiers n'existent pas encore, on attend que Spark démarre
    if not os.path.exists(vertices_path) or not os.path.exists(edges_path):
        return [], "En attente des données Spark..."

    with open(vertices_path) as f:
        vertices = json.load(f)
    with open(edges_path) as f:
        edges = json.load(f)

    elements = []

    # ── Construction des nœuds Cytoscape ──
    for v in vertices:
        pagerank = v.get("pagerank", 0)
        # Taille entre 20px et 70px, proportionnelle au PageRank
        size = max(20, min(70, 20 + pagerank * 30))
        elements.append({
            "data": {
                "id":           v["id"],
                "label":        v.get("label", v["id"]),
                "type":         v["type"],
                "size":         size,
                "pagerank":     pagerank,
                "relationship": "",
            },
            "classes": v["type"]  # classe CSS → détermine la couleur du nœud
        })

    # ── Construction des arêtes Cytoscape ──
    for e in edges:
        elements.append({
            "data": {
                # ID unique = combinaison src + dst + type de relation
                "id":           f"{e['src']}__{e['dst']}__{e['relationship']}",
                "source":       e["src"],
                "target":       e["dst"],
                "relationship": e["relationship"],
            },
            "classes": e["relationship"]  # classe CSS → détermine la couleur de l'arête
        })

    # ── Calcul des statistiques affichées en bas ──
    components    = set(v.get("component_id", v["id"]) for v in vertices)
    nb_components = len(components)

    # Top 3 des nœuds par score PageRank
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
