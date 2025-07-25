#!/usr/bin/env python3
"""
Control D Sync (multi-profile)
----------------------
Sincroniza múltiplos perfis do Control D com listas públicas do Hagezi.

Para cada perfil:
1. Lê as pastas das listas.
2. Remove as pastas existentes com os mesmos nomes.
3. Cria as pastas novamente.
4. Adiciona os domínios bloqueados em lotes.

Configure seu arquivo `.env` com:
- PROFILE_1_ID
- PROFILE_1_TOKEN
- PROFILE_2_ID
- PROFILE_2_TOKEN
...
"""

import os
import logging
from typing import Dict, List

import httpx
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# 0. Bootstrap – load secrets and configure logging
# --------------------------------------------------------------------------- #
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("control-d-sync")

# --------------------------------------------------------------------------- #
# 1. Constants – tweak only here
# --------------------------------------------------------------------------- #
API_BASE = "https://api.controld.com/profiles"

FOLDER_URLS = [
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/controld/spam-tlds-folder.json",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/controld/spam-tlds-allow-folder.json",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/controld/ultimate-known_issues-allow-folder.json",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/controld/spam-idns-folder.json",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/controld/native-tracker-lgwebos-folder.json",
]

BATCH_SIZE = 500

# --------------------------------------------------------------------------- #
# 2. Clients
# --------------------------------------------------------------------------- #
_api = httpx.Client(timeout=30)
_gh = httpx.Client(timeout=30)
_cache: Dict[str, Dict] = {}

# --------------------------------------------------------------------------- #
# 3. Helpers
# --------------------------------------------------------------------------- #
def _api_get(url: str) -> httpx.Response:
    r = _api.get(url)
    r.raise_for_status()
    return r

def _api_delete(url: str) -> httpx.Response:
    r = _api.delete(url)
    r.raise_for_status()
    return r

def _api_post(url: str, data: Dict) -> httpx.Response:
    r = _api.post(url, data=data)
    r.raise_for_status()
    return r

def _gh_get(url: str) -> Dict:
    if url not in _cache:
        r = _gh.get(url)
        r.raise_for_status()
        _cache[url] = r.json()
    return _cache[url]

def list_existing_folders(profile_id: str) -> Dict[str, str]:
    data = _api_get(f"{API_BASE}/{profile_id}/groups").json()
    folders = data.get("body", {}).get("groups", [])
    return {
        f["group"].strip().lower(): f["PK"]
        for f in folders
        if f.get("group") and f.get("PK")
    }

def fetch_folder_name(url: str) -> str:
    return _gh_get(url)["group"]["group"].strip().lower()

def delete_folder(profile_id: str, name: str, folder_id: str) -> None:
    _api_delete(f"{API_BASE}/{profile_id}/groups/{folder_id}")
    log.info("Deleted folder '%s' (ID %s)", name, folder_id)

def create_folder(profile_id: str, name: str, do: int, status: int) -> str:
    _api_post(
        f"{API_BASE}/{profile_id}/groups",
        data={"name": name, "do": do, "status": status},
    )
    data = _api_get(f"{API_BASE}/{profile_id}/groups").json()
    for grp in data["body"]["groups"]:
        if grp["group"].strip().lower() == name.strip().lower():
            log.info("Created folder '%s' (ID %s)", name, grp["PK"])
            return str(grp["PK"])
    raise RuntimeError(f"Folder '{name}' was not found after creation")

def push_rules(
    profile_id: str,
    folder_name: str,
    folder_id: str,
    do: int,
    status: int,
    hostnames: List[str],
) -> None:
    for i, start in enumerate(range(0, len(hostnames), BATCH_SIZE), 1):
        batch = hostnames[start : start + BATCH_SIZE]
        _api_post(
            f"{API_BASE}/{profile_id}/rules",
            data={
                "do": do,
                "status": status,
                "group": folder_id,
                "hostnames[]": batch,
            },
        )
        log.info(
            "Folder '%s' – batch %d: added %d rules",
            folder_name,
            i,
            len(batch),
        )
    log.info("Folder '%s' – finished (%d total rules)", folder_name, len(hostnames))

# --------------------------------------------------------------------------- #
# 4. Main sync logic per profile
# --------------------------------------------------------------------------- #
def sync_profile(profile_id: str, token: str) -> None:
    log.info("Starting sync for profile: %s", profile_id)

    _api.headers["Authorization"] = f"Bearer {token}"

    wanted_names = [fetch_folder_name(u) for u in FOLDER_URLS]
    existing = list_existing_folders(profile_id)

    for name in wanted_names:
        if name in existing:
            delete_folder(profile_id, name, existing[name])

    for url in FOLDER_URLS:
        js = _gh_get(url)
        grp = js["group"]
        folder_name = grp["group"]
        action = grp.get("action", {})
        if "do" not in action or "status" not in action:
            raise ValueError(f"Invalid action block in list from {url}: {action}")
        do = action["do"]
        status = action["status"]
        hostnames = [r["PK"] for r in js.get("rules", []) if r.get("PK")]

        folder_id = create_folder(profile_id, folder_name, do, status)
        if hostnames:
            push_rules(profile_id, folder_name, folder_id, do, status, hostnames)
        else:
            log.info("Folder '%s' - no rules to push", folder_name)

    log.info("✅ Sync complete for profile: %s", profile_id)

# --------------------------------------------------------------------------- #
# 5. Entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    PROFILES = [
        {"id": os.getenv("PROFILE_1_ID"), "token": os.getenv("PROFILE_1_TOKEN")},
        {"id": os.getenv("PROFILE_2_ID"), "token": os.getenv("PROFILE_2_TOKEN")},
        {"id": os.getenv("PROFILE_3_ID"), "token": os.getenv("PROFILE_3_TOKEN")},
    ]

    for profile in PROFILES:
        pid = profile.get("id")
        token = profile.get("token")
        if not pid or not token:
            log.warning("⚠️  Skipping profile with missing ID or token.")
            continue
        try:
            sync_profile(pid, token)
        except Exception as e:
            log.error("❌ Failed to sync profile %s: %s", pid, e)
