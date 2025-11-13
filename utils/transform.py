import pandas as pd
from utils.helpers import extrair_estado, extrair_cidade, classificar_status_uso

def build_dataframe(networks, devices, usage):
    df = pd.DataFrame(networks)

    df["Estado"] = df["id"].apply(extrair_estado)
    df["Cidade"] = df["tags"].apply(extrair_cidade)
    df["TipoRede"] = df["name"].apply(lambda x: "Estadual" if "EE" in x else "Municipal")

    usage_df = pd.DataFrame(usage)[["networkId", "usage"]]
    df = df.merge(usage_df, left_on="id", right_on="networkId", how="left")

    df.rename(columns={"usage":"Uso_Total_GB"}, inplace=True)

    df["Uso_por_Device"] = df["Uso_Total_GB"] / df["deviceCount"]
    df["Status_Utilizacao"] = df["Uso_Total_GB"].apply(classificar_status_uso)

    final = df[[
        "id", "name", "TipoRede", "Cidade", "Estado",
        "deviceCount", "clientCount", "Uso_Total_GB",
        "Uso_por_Device", "Status_Utilizacao"
    ]]

    final.columns = [
        "inep", "escola", "tiporede", "cidade", "estado",
        "devices", "clients", "usage_gb", "uso_por_device", "status_utilizacao"
    ]

    return final
