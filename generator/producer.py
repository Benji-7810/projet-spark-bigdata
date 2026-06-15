import socket
import json
import random
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────
# DONNÉES SIMULÉES
# Pools fixes utilisés pour générer des événements aléatoires
# ─────────────────────────────────────────
USERS    = [f"usr_{i:04d}" for i in range(1, 6)]    # 5 utilisateurs fictifs
SELLERS  = [f"sel_{i:04d}" for i in range(1, 4)]    # 3 vendeurs fictifs
PRODUCTS = [f"prod_{i:04d}" for i in range(1, 9)]   # 8 produits fictifs
CITIES      = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille"]
CATEGORIES  = ["Véhicules", "Électronique", "Immobilier", "Mode", "Maison", "Sports", "Loisirs"]
ACTIONS     = ["AIME", "VOUT", "ACHAT"]


def generate_event():
    """
    Génère un événement JSON simulant une interaction sur LeBonCoin.
    Les actions ne sont PAS uniformes : AIME=60%, VOUT=30%, ACHAT=10%
    (plus réaliste : on aime plus qu'on achète).
    """
    return {
        "timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id":     random.choice(USERS),
        "user_city":   random.choice(CITIES),
        "product_id":  random.choice(PRODUCTS),
        "product_cat": random.choice(CATEGORIES),
        "seller_id":   random.choice(SELLERS),
        "action_type": random.choices(ACTIONS, weights=[60, 30, 10])[0],  # tirage pondéré
        "price":       round(random.uniform(5.0, 2000.0), 2)
    }


# ─────────────────────────────────────────
# SERVEUR TCP
# On crée un socket serveur qui attend la connexion de Spark
# ─────────────────────────────────────────
HOST = "localhost"
PORT = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # réutilise le port si déjà utilisé
server.bind((HOST, PORT))
server.listen(1)  # accepte 1 seul client à la fois (Spark)

print(f"[PRODUCER] En attente de connexion sur {HOST}:{PORT}...")

# server.accept() est BLOQUANT : le script attend ici sans rien faire
# jusqu'à ce que pipeline.py se connecte → toujours démarrer le producer EN PREMIER
conn, addr = server.accept()
print(f"[PRODUCER] Spark connecté depuis {addr}, démarrage du flux...")


# ─────────────────────────────────────────
# BOUCLE D'ENVOI
# Envoie un événement JSON toutes les 6 secondes
# ─────────────────────────────────────────
try:
    while True:
        event = generate_event()

        # Spark lit le socket ligne par ligne → on termine chaque message par "\n"
        msg = json.dumps(event) + "\n"
        conn.sendall(msg.encode("utf-8"))

        print(f"[SENT] {event['action_type']} | {event['user_id']} -> {event['product_id']}")

        time.sleep(1.0)  # 1 événement par seconde → flux dense, windowing significatif

except (BrokenPipeError, ConnectionResetError):
    # Spark s'est déconnecté (pipeline arrêté) → on ferme proprement
    print("[PRODUCER] Spark déconnecté.")
finally:
    conn.close()
    server.close()
