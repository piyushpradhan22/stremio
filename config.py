import os
import time
import requests

API_URL = "https://api.github.com/repos/piyushpradhan22/credentials/contents/credentials.json"
RAW_URL = "https://raw.githubusercontent.com/piyushpradhan22/credentials/refs/heads/main/credentials.json"

def load_credentials():
    token = os.getenv("GITHUB_TOKEN") or os.getenv("token")
    if not token:
        raise RuntimeError("Missing 'GITHUB_TOKEN' (or 'token') environment variable to load credentials from repository.")

    print("[Config] Loading credentials from GitHub repository...")
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.raw"
    }

    try:
        res = requests.get(API_URL, headers=headers, timeout=15)
        if res.status_code == 200:
            data = res.json()
        else:
            raise RuntimeError(f"API returned {res.status_code}")
    except Exception as e:
        print(f"[Config] API fetch notice ({e}), trying raw fallback...")
        res = requests.get(
            f"{RAW_URL}?t={int(time.time())}",
            headers={"Authorization": f"token {token}"},
            timeout=15
        )
        if res.status_code != 200:
            raise RuntimeError(f"Failed to fetch credentials: {res.status_code} - {res.text}")
        data = res.json()

    for k, v in data.items():
        if k and v:
            os.environ[k] = str(v)

    print(f"[Config] Successfully loaded {len(data)} credentials from repository.")
    return data

creds = load_credentials()

QBITTORRENT_URL = creds["qb_url"]
QBITTORRENT_USER = creds["username"]
QBITTORRENT_PASSWORD = creds.get("qb_password") or creds["password"]

WEBDAV_URL = creds["webdav_url"].rstrip('/')
WEBDAV_USER = creds.get("webdav_user", creds.get("username", "admin"))
WEBDAV_PASSWORD = creds.get("webdav_password") or creds.get("qb_password") or creds["password"]

ADDON_URL = creds.get("addon_url")