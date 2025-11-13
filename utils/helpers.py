import re
import pandas as pd
from datetime import datetime, timedelta, timezone

# -------------------------
# 1. Extrair TAG específica
# -------------------------
def extrair_tag(tags, prefixo):
    """Retorna a primeira tag que começa com o prefixo (ex.: '3' para cidade)."""
    if not tags or not isinstance(tags, list):
        return None
    for t in tags:
        if isinstance(t, str) and t.startswith(prefixo):
            return t
    return None

# -------------------------
# 2. Extrair CIDADE pela TAG 3 (fonte oficial)
# -------------------------
def extrair_cidade_de_tags(tags):
    """
    Extrai somente a cidade baseada na TAG 3.
    Exemplo: '3juazeiro' -> 'juazeiro' -> 'Juazeiro'
    """
    tag = extrair_tag(tags, "3")
    if not tag:
        return None

    bruto = tag[1:]  # Remove o "3"
    bruto = bruto.strip()

    # Mantém apenas letras (com acentos)
    m = re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]+", bruto)
    cidade = m.group(0) if m else bruto

    return cidade.strip().capitalize()

# -------------------------
# 3. Extrair Tipo de Rede
# -------------------------
def extrair_tipo_rede_de_tags(tags):
    if not tags or not isinstance(tags, list):
        return None

    texto = " ".join(t.lower() for t in tags if isinstance(t, str))

    if "municipal" in texto:
        return "Municipal"
    if "estadual" in texto:
        return "Estadual"

    return None

# -------------------------
# 4. Calcular offline e %
# -------------------------
def conta_offline_e_pct(device_statuses):
    total = len(device_statuses or [])
    if total == 0:
        return 0, 0.0

    online = 0

    for d in device_statuses:
        status = d.get("status")
        if status == "online":
            online += 1
        elif status == "alerting":
            try:
                dt = d.get("lastReportedAt")
                if dt:
                    dt_dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                    if datetime.now(timezone.utc) - dt_dt < timedelta(days=7):
                        online += 1
            except:
                pass

    offline = total - online
    pct = (offline / total * 100) if total > 0 else 0
    return offline, round(pct, 2)

# -------------------------
# 5. Classificar uso
# -------------------------
def classificar_status(uso_por_ap_gb, clients):
    if (uso_por_ap_gb or 0) <= 5 and (clients or 0) < 30:
        return "Baixo uso"
    elif (uso_por_ap_gb or 0) <= 10 and (clients or 0) < 100:
        return "Médio uso"
    return "Uso normal"

# -------------------------
# 6. Mapear cidade → estado
# -------------------------
def mapear_estado(cidade):
    if not cidade:
        return None

    c = cidade.lower()

    mapa = {
        "alenquer": "PA - Pará",
        "almeirim": "PA - Pará",
        "caico": "RN - Rio Grande do Norte",
        "campinagrande": "PB - Paraíba",
        "caruaru": "PE - Pernambuco",
        "juazeiro": "BA - Bahia",
        "macapa": "AP - Amapá",
        "montealegre": "PA - Pará",
        "mossoro": "RN - Rio Grande do Norte",
        "petrolina": "PE - Pernambuco",
        "santarem": "PA - Pará",
    }

    return mapa.get(c)

# -------------------------
# 7. Dividir Nome (INEP e Escola)
# -------------------------
def dividir_nome(df):
    dividido = df["Name"].str.split("-", n=1, expand=True)
    df["Inep"] = dividido[0].str.strip()
    df["Escola"] = dividido[1].str.strip()
    return df

# -------------------------
# 8. TRATAMENTO FINAL
# -------------------------
def tratar_dataframe(df):
    """
    Aplica todas as transformações finais equivalentes ao Código M + Colab.
    Agora, CIDADE é SEMPRE da tag 3.
    """
    # Garantir que tags exista
    df["tags"] = df["tags"].apply(lambda x: x if isinstance(x, list) else [])

    # Dividir nome
    df = dividir_nome(df)

    # Cidade oficial — pela TAG 3
    df["Cidade"] = df["tags"].apply(extrair_cidade_de_tags)

    # Estado baseado na cidade
    df["Estado"] = df["Cidade"].apply(mapear_estado)

    # Tipo de rede pela tag
    df["TipoRede"] = df["tags"].apply(extrair_tipo_rede_de_tags)

    # Status de utilização
    df["Status_Utilizacao"] = df.apply(
        lambda row: classificar_status(row.get("Uso_por_AP_GB"), row.get("Clientes_por_AP")),
        axis=1
    )

    # Reordenação final
    colunas = [
        "Inep", "Escola", "TipoRede", "Cidade", "Estado",
        "Devices", "Offline devices", "Offline_pct",
        "Clients", "Usage_GB", "Uso_por_AP_GB",
        "Clientes_por_AP", "Status_Utilizacao", "Data_Coleta"
    ]

    df = df[colunas]

    return df
