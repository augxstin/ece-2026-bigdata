import os
import urllib.request
import json

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = os.environ.get("TRINO_PORT", "8080")

url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info"

print(f"Vérification de Trino sur {url}...")
try:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=5) as response:
        if response.status == 200:
            data = json.loads(response.read().decode('utf-8'))
            print("--- Trino est UP et accessible ---")
            print(f"Version: {data.get('nodeVersion', {}).get('version', 'N/A')}")
        else:
            print(f"Trino a répondu avec le code HTTP: {response.status}")
except Exception as e:
    print(f"Info Trino (Simulation d'architecture): {e}")
