import aiohttp
import asyncio
import time
from utils.config import API_KEY
from utils.logger import get_logger

logger = get_logger("api_async")

BASE_URL = "https://api.meraki.com/api/v1"

HEADERS = {
    "X-Cisco-Meraki-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# ======================================================
# 🔄 Cliente HTTP assíncrono
# ======================================================

async def fetch(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=20) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logger.error(f"HTTP {resp.status} → {url}")
                return None
    except Exception as e:
        logger.error(f"Erro em fetch(): {e}")
        return None

# ======================================================
# 📍 Obter Organização
# ======================================================

async def get_org_id_by_name(org_name):
    async with aiohttp.ClientSession() as session:
        url = f"{BASE_URL}/organizations"
        orgs = await fetch(session, url)
        if not orgs:
            return None

        for org in orgs:
            if org["name"].strip().lower() == org_name.lower():
                return org["id"]
        return None

# ======================================================
# 📡 Redes com cache
# ======================================================

_CACHE_NETWORKS = {"data": None, "timestamp": 0, "ttl": 300}

async def get_networks_cached(org_id):
    agora = time.time()

    if _CACHE_NETWORKS["data"] and (agora - _CACHE_NETWORKS["timestamp"] < _CACHE_NETWORKS["ttl"]):
        return _CACHE_NETWORKS["data"]

    async with aiohttp.ClientSession() as session:
        url = f"{BASE_URL}/organizations/{org_id}/networks"
        redes = await fetch(session, url)

    _CACHE_NETWORKS["data"] = redes
    _CACHE_NETWORKS["timestamp"] = agora
    return redes

# ======================================================
# 🔍 Dados das redes
# ======================================================

async def get_network_clients(session, network_id):
    url = f"{BASE_URL}/networks/{network_id}/clients?perPage=100"
    data = await fetch(session, url)
    return data if isinstance(data, list) else []

async def get_network_traffic(session, network_id):
    url = f"{BASE_URL}/networks/{network_id}/traffic"
    data = await fetch(session, url)
    return data if isinstance(data, list) else []

# ======================================================
# 🔥 Processar cada rede (assíncrono)
# ======================================================

async def process_network(session, org_id, network):

    try:
        net_id = network["id"]
        name = network.get("name", "")

        # ----- Rodar 2 endpoints em paralelo -----
        clients, traffic = await asyncio.gather(
            get_network_clients(session, net_id),
            get_network_traffic(session, net_id)
        )

        total_clients = len(clients)
        total_bytes = sum(item.get("sent", 0) + item.get("recv", 0) for item in traffic)

        usage_gb = round(total_bytes / (1024 ** 3), 2)

        return {
            "Name": name,
            "Devices": 1,
            "Offline devices": 0,
            "Offline_pct": 0,
            "Clients": total_clients,
            "Usage_GB": usage_gb,
            "Uso_por_AP_GB": usage_gb,
            "Clientes_por_AP": total_clients,
            "tags": network.get("tags", [])   # necessário para extrair cidade
        }

    except Exception as e:
        logger.error(f"Erro em process_network → {network.get('name')} | {e}")
        return None
