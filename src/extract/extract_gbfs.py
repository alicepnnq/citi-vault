# temps réel GBFS (station info + status)

import requests, os, json, time
from datetime import datetime

BASE = "https://gbfs.citibikenyc.com/gbfs/en"  # GBFS en anglais

ENDPOINTS = {
    "station_information": f"{BASE}/station_information.json",
    "station_status": f"{BASE}/station_status.json",
    "system_information": f"{BASE}/system_information.json",
    "system_regions": f"{BASE}/system_regions.json",
}

def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def extract_gbfs_once(ts=None):
    ts = ts or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = f"data/raw/gbfs/{ts}"
    for name, url in ENDPOINTS.items():
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        payload = r.json()
        save_json(payload, f"{out_dir}/{name}.json")
        print(f"✅ {name} saved -> {out_dir}/{name}.json")

if __name__ == "__main__":
    extract_gbfs_once()
