import boto3
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(layout="wide")

def tz_brazil_now():
    return datetime.now(timezone(timedelta(hours=-3)))

dt_brasil = tz_brazil_now().strftime("%Y-%m-%d")

load_dotenv()


# --- CSS ---
def load_css():
    with open("style.css") as f:
        css = f"<style>{f.read()}</style>"
        st.markdown(css, unsafe_allow_html=True)

load_css()


# ------------------------------- DOWNLOAD CSV  --------------------------- #

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_AK"),
    aws_secret_access_key=os.getenv("AWS_SAK"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)

# Initialize the S3 client
s3 = session.client('s3')

bucket_name = os.getenv("S3_BUCKET")
s3_object_key = f'outputs/{dt_brasil}/omqb_results.csv' 

# Define the local path where you want to save the downloaded file
local_file_path = 'omqb_results.csv'

def get_etag(bucket, key):
    """Retorna o ETag do arquivo no S3 para invalidar o cache automaticamente."""
    head = s3.head_object(Bucket=bucket, Key=key)
    return head["ETag"]

@st.cache_data
def download_csv_from_s3(etag, key):
    """
    Baixa o CSV do S3 somente se o ETag mudar.
    """
    local_file_path = "omqb_results.csv"
    s3.download_file(bucket_name, key, local_file_path)
    return local_file_path

print(f"Downloading '{s3_object_key}' to '{local_file_path}'.")
etag = get_etag(bucket_name, s3_object_key)
local_file_path = download_csv_from_s3(etag, s3_object_key)


@st.cache_data
def load_history():
    obj = s3.get_object(Bucket=bucket_name, Key="history/full_history.csv")
    return pd.read_csv(obj["Body"])

# ------------------------------- TRATAR CSV ENTRADAS DO DIA  --------------------------- #

df = pd.read_csv('omqb_results.csv')

# Convert column to datetime
df["Date"] = pd.to_datetime(df["Date"])
df["Data"] = df["Date"].dt.date
df["Hora"] = df["Date"].dt.time

# Format
df["Date"] = df["Date"].dt.strftime("%d/%m/%Y %H:%M")

df = df[df['Back_Model'] != "Sugestão: Fique de fora no modelo"]

df["Entrada"] = np.where(df['Back_Model'].str.startswith("Sugestão: Back Home"), df["Home"], np.where(df['Back_Model'].str.startswith("Sugestão: Back Away"), df["Away"], None))
df["Odd Sugerida"] = np.where(df['Back_Model'].str.startswith("Sugestão: Back Home"), df["Odd_Back_H"], np.where(df['Back_Model'].str.startswith("Sugestão: Back Away"), df["Odd_Back_A"], None))

df_filtrado = df[["Data", "League", "Home", "Away", "Entrada", "Odd Sugerida"]]
df_filtrado["Odd Sugerida"] = df_filtrado["Odd Sugerida"].apply(
    lambda x: f'<span class="odd-badge">{x}</span>'
)


# ------------------------------- Histórico ------------------------ #

df_history = load_history()
df_history.sort_values("DataExecucao")
df_history["Entrada"] = range(1, len(df_history) + 1)

# ---------- GRÁFICO PLOTLY ----------
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_history["Entrada"],
    y=df_history["PL_Acumulado"],
    mode="lines",
    hovertemplate="Entrada: %{x}<br>P/L Acumulado: %{y:.2f}<extra></extra>"
))

fig.update_layout(
    height=420,
    margin=dict(l=30, r=30, t=50, b=30),
    title="Evolução do P/L Acumulado",
    xaxis_title="Número de Entradas",
    yaxis_title="P/L Acumulado",
    hovermode="x unified"
)


# ------------------------------- PÁGINA --------------------------- #


st.title("MM Tips")
st.write("Entradas do dia")

# --- TABELA HTML ---
html = "<table class='custom-table'>"

# Cabeçalho
html += "<thead><tr>"
for col in df_filtrado.columns:
    html += f"<th>{col}</th>"
html += "</tr></thead>"

# Conteúdo
html += "<tbody>"
for _, row in df_filtrado.iterrows():
    html += "<tr>"
    for col in df_filtrado.columns:
        if col == "Odd Sugerida":
            html += f"<td><span class='odd-badge'>{row[col]}</span></td>"
        elif col == "Entrada":
            html += f"<td><b>{row[col]}</b></td>"
        else:
            html += f"<td>{row[col]}</td>"
    html += "</tr>"
html += "</tbody></table>"

st.markdown(html, unsafe_allow_html=True)
st.write("")
st.title("📈 Performance Histórica - MM Tips (Desde 08/out/2025)")
st.plotly_chart(fig, use_container_width=True)

st.title("As entradas diárias ficarão disponíveis gratuitamente por algum período. Após isso, será necessário assinar o serviço.")