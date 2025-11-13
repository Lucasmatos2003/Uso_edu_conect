import pandas as pd
import numpy as np
from meraki import DashboardAPI
from datetime import datetime
from sqlalchemy import create_engine, text

# Imports do projeto
from utils.config import API_KEY, PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT
from utils.helpers import tratar_dataframe
from utils.db import ensure_table_exists, insert_into_db

print("🚀 Iniciando ETL Meraki → infra_analytics_db (modo incremental)...\n")

# ==========================================================
# 1) Conectar à API
# ==========================================================
dashboard = DashboardAPI(
    api_key=API_KEY,
    suppress_logging=True,
    wait_on_rate_limit=True
)

# Nome fixo da organização (apenas aqui)
ORG_NAME = "RNP - Projeto Educação Conectada"

orgs = dashboard.organizations.getOrganizations()
org_id = next((o["id"] for o in orgs if o["name"] == ORG_NAME), None)

if not org_id:
    raise Exception("❌ Organização Meraki não encontrada.")

redes = dashboard.organizations.getOrganizationNetworks(org_id)
print(f"🔍 Redes encontradas: {len(redes)}\n")

dados = []

# ==========================================================
# 2) LOOP DAS REDES
# ==========================================================
for i, rede in enumerate(redes, start=1):
    name = rede.get("name", "")
    tags = rede.get("tags", [])

    # Buscar status dos devices
    try:
        devices_status = dashboard.organizations.getOrganizationDevicesStatuses(
            organizationId=org_id,
            networkIds=[rede["id"]]
        )
    except Exception as e:
        print(f"⚠️ Erro ao obter devices da rede {name}: {e}")
        devices_status = []

    devices_total = len(devices_status)
    offline_devices = sum(1 for d in devices_status if d.get("status") != "online")
    offline_pct = round((offline_devices / devices_total * 100), 2) if devices_total else 0

    # Simular uso (para manter compatibilidade com seu código atual)
    usage_gb = np.random.uniform(0.1, 10.0)
    clients = np.random.randint(1, 500)

    uso_por_ap = usage_gb / devices_total if devices_total else 0
    clientes_por_ap = clients / devices_total if devices_total else 0

    dados.append({
        "Name": name,
        "tags": tags,
        "Devices": devices_total,
        "Offline devices": offline_devices,
        "Offline_pct": offline_pct,
        "Clients": clients,
        "Usage_GB": round(usage_gb, 2),
        "Uso_por_AP_GB": round(uso_por_ap, 2),
        "Clientes_por_AP": round(clientes_por_ap, 2),
        "Data_Coleta": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    print(f"[{i}/{len(redes)}] {name} — OK")

# ==========================================================
# 3) DataFrame FINAL
# ==========================================================
df = pd.DataFrame(dados)

df = tratar_dataframe(df)   # ← usa SEU helpers.py, sem alteração

# ==========================================================
# 4) Criar tabela caso não exista
# ==========================================================
ensure_table_exists()

# ==========================================================
# 5) Inserir incremental
# ==========================================================
insert_into_db(df)

print("💾 Dados enviados ao banco!")

# ==========================================================
# 6) Log local
# ==========================================================
arquivo_log = f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
df.to_csv(arquivo_log, index=False, encoding="utf-8-sig")

print(f"🗂️ Log salvo: {arquivo_log}")
print("\n🏁 ETL finalizado com sucesso!")
