# Cria pasta dados se não existir
pasta = "dados"
if not os.path.exists(pasta):
  os.makedirs(pasta)

"""## BCB/SGS"""

# Salva dados como arquivo .parquet
for df in df_tratado_bcb_sgs.items():
  df[1].to_parquet(f"{pasta}/df_bcb_sgs_{df[0]}.parquet")

"""## BCB/ODATA"""

# Salva dados como arquivo .parquet
df_tratado_bcb_odata_mensal.to_parquet(f"{pasta}/df_bcb_odata_mensal.parquet")
df_tratado_bcb_odata_pib.set_index("data").to_parquet(f"{pasta}/df_bcb_odata_trimestral.parquet")

"""## IPEADATA"""

# Salva dados como arquivo .parquet
for df in df_tratado_ipeadata.items():
  df[1].to_parquet(f"{pasta}/df_ipeadata_{df[0]}.parquet")

"""## IBGE/SIDRA"""

# Salva dados como arquivo .parquet
for df in df_tratado_ibge_sidra.items():
  df[1].query("index >= '2000-01-01'").to_parquet(f"{pasta}/df_ibge_sidra_{df[0]}.parquet")

"""## FRED"""

# Salva dados como arquivo .parquet
for df in df_tratado_fred.items():
  df[1].query("index >= '2000-01-01'").to_parquet(f"{pasta}/df_fred_{df[0]}.parquet")

"""## IFI"""

# Salva dados como arquivo .parquet
df_tratado_ifi.to_parquet(f"{pasta}/df_ifi_trimestral.parquet")

"""## Tabelas por frequência"""

# Diária
df_diaria = (
    df_tratado_bcb_sgs["Diária"]
    .join(
        other=df_tratado_ipeadata["Diária"].reset_index().assign(
            data=lambda x: pd.to_datetime(x['data'].dt.strftime("%Y-%m-%d"))
        ).set_index("data"),
        how="outer"
    )
    .join(other=df_tratado_fred["Diária"], how="outer")
    .reset_index()
    .assign(data=lambda x: pd.to_datetime(x['data']))
    .query("data >= @pd.to_datetime('2000-01-01')")
    .set_index('data')
)
df_diaria.to_parquet(f"{pasta}/df_diaria.parquet")

# Mensal
temp_lista = [
    df_tratado_bcb_sgs["Mensal"],
    df_tratado_bcb_odata_mensal,
    df_tratado_ipeadata["Mensal"],
    df_tratado_ibge_sidra["Mensal"],
    df_tratado_fred["Mensal"]
]

df_mensal = (
  temp_lista[0]
  .join(other = temp_lista[1:], how = "outer")
  .query("index >= @pd.to_datetime('2000-01-01')")
  .astype(float)
  )
df_mensal.to_parquet(f"{pasta}/df_mensal.parquet")

# Trimestral
temp_lista = [
    df_tratado_bcb_sgs["Trimestral"],
    df_tratado_bcb_odata_pib.set_index("data"),
    df_tratado_ibge_sidra["Trimestral"],
    df_tratado_fred["Trimestral"],
    df_tratado_ifi
]

df_trimestral = (
  temp_lista[0]
  .join(other = temp_lista[1:], how = "outer")
  .query("index >= @pd.to_datetime('2000-01-01')")
  .astype(float)
)
df_trimestral.index = pd.to_datetime(df_trimestral.index)
df_trimestral.to_parquet(f"{pasta}/df_trimestral.parquet")

# Anual
df_anual = (
  df_tratado_bcb_sgs["Anual"]
  .query("index >= @pd.to_datetime('2000-01-01')")
  .astype(float)
)
df_anual.to_parquet(f"{pasta}/df_anual.parquet")
