import socket
import json
import random
import time
from datetime import datetime, timezone

# --- Données simulées ---
USERS = [f"usr_{i:04d}" for i in range(1, 21)]
SELLERS = [f"sel_{i:04d}" for i in range(1, 11)]
PRODUCTS = [f"prod_{i:04d}" for i in range(1, 31)]
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille"]
CATEGORIES = ["Véhicules", "Électronique", "Immobilier", "Mode", "Maison", "Sports", "Loisirs"]
ACTIONS = ["AIME", "VOUT", "ACHAT"]

def generate_event():
    return {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": random.choice(USERS),
        "user_city": random.choice(CITIES),
        "product_id": random.choice(PRODUCTS),
        "product_cat": random.choice(CATEGORIES),
        "seller_id": random.choice(SELLERS),
        "action_type": random.choices(ACTIONS, weights=[60, 30, 10])[0],
        "price": round(random.uniform(5.0, 2000.0), 2)
    }

# --- Serveur socket ---
HOST = "localhost"
PORT = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(1)

print(f"[PRODUCER] En attente de connexion sur {HOST}:{PORT}...")
conn, addr = server.accept()
print(f"[PRODUCER] Spark connecté depuis {addr}, démarrage du flux...")

try:
    while True:
        event = generate_event()
        msg = json.dumps(event) + "\n"
        conn.sendall(msg.encode("utf-8"))
        print(f"[SENT] {event['action_type']} | {event['user_id']} -> {event['product_id']}")
        time.sleep(3.0)
except (BrokenPipeError, ConnectionResetError):
    print("[PRODUCER] Spark déconnecté.")
finally:
    conn.close()
    server.close()