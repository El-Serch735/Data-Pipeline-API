import streamlit as st
import requests
import pandas as pd
import plotly.express as px 
import os
from dotenv import load_dotenv

load_dotenv()
AUTH_KEY = os.getenv("APP_AUTH_KEY")

st.set_page_config(page_title="Nuclear Outages Monitor", layout="wide")
st.title("☢️ Nuclear Outages Dashboard")

# 1. BARRA LATERAL
st.sidebar.header("Control de Datos")

if st.sidebar.button("🔄 Actualizar datos"):
    # Definimos el header de seguridad para el POST
    auth_header = {"X-API-KEY": AUTH_KEY}
    
    with st.status("Solicitando actualización...", expanded=True) as status:
        try:
            res = requests.post("http://localhost:8000/refresh", headers=auth_header)
            if res.status_code == 200:
                st.toast("✅ Proceso iniciado", icon='🚀')
                status.update(label="Actualización en curso (segundo plano)", state="complete")
            elif res.status_code == 401:
                st.sidebar.error("❌ Error 401: X-API-KEY no válida.")
                status.update(label="Fallo de autenticación", state="error")
            else:
                st.sidebar.warning(f"Error: {res.status_code}")
        except Exception as e:
            st.sidebar.error(f"Error de conexión: {e}")

st.sidebar.divider()
limit = st.sidebar.number_input("Registros a traer", 10, 5000, 500)
facility_filter = st.sidebar.text_input("Filtrar por Planta")

# 2. CONSUMO DE API (GET)
try:
    params = {"limit": limit}
    if facility_filter:
        params["facility"] = facility_filter
        
    response = requests.get("http://localhost:8000/data", params=params)
    
    if response.status_code == 200:
        data_json = response.json().get("data", [])
        if not data_json:
            st.info("ℹ️ Sin resultados para este filtro.")
        else:
            df = pd.DataFrame(data_json)
            
            # --- SECCIÓN DE GRÁFICA ---
            st.subheader("📊 Top 10 Plantas con mayor Outage (MW)")
            # Aseguramos que 'outage' sea numérico y eliminamos nulos
            df['outage'] = pd.to_numeric(df['outage'], errors='coerce').fillna(0)
            # Agrupamos por planta para la gráfica
            chart_data = df.groupby('facilityName')['outage'].sum().sort_values(ascending=False).head(10).reset_index()
            chart_data['outage'] = chart_data['outage'].round(2) # Redondeo para mejorar la visualización
            fig = px.bar(
                chart_data, 
                x='facilityName', 
                y='outage', 
                color='outage',
                text_auto='.2s', # Muestra el número simplificado sobre la barra
                labels={'outage':'MW Fuera', 'facilityName': 'Planta'}
            )

            fig.update_layout(yaxis_type='linear') 
            st.plotly_chart(fig, use_container_width=True)
            
            # Tabla
            st.subheader("📋 Detalle de Registros")
            st.dataframe(df, use_container_width=True)
    else:
        st.error(f"Error API: {response.status_code}")
except:
    st.error("❌ Backend desconectado.")
