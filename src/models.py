import pandas as pd
import os
from deltalake import DeltaTable

def process_data_model():
    delta_path = "data/nuclear_outages_delta"
    if not os.path.exists(delta_path):
        print("Error: No existe la Delta Table. Ejecuta el conector primero.")
        return

    # Cargamos desde Delta (Bonus)
    df = DeltaTable(delta_path).to_pandas()
    
    # Limpieza de nombres de columnas (sustituir guiones por guiones bajos para SQL)
    df.columns = [c.replace('-', '_') for c in df.columns]
    
    # 1. Dim_Facilities
    dim_facilities = df[['facilityName', 'facility', 'generator']].drop_duplicates()
    
    # 2. Dim_Calendar
    df['period_dt'] = pd.to_datetime(df['period'])
    calendar_dates = pd.Series(df['period_dt'].unique())
    dim_calendar = pd.DataFrame({
        'period': calendar_dates,
        'year': calendar_dates.dt.year,
        'month': calendar_dates.dt.month,
        'quarter': calendar_dates.dt.quarter
    })
    
    # 3. Fact_Outages (La tabla que consume la API)
    # Nos aseguramos de que 'outage' sea numérico aquí también
    df['outage'] = pd.to_numeric(df['outage'], errors='coerce').fillna(0)
    fact_outages = df[['period', 'facilityName', 'outage', 'outage_units']]
    
    # Guardar resultados
    os.makedirs("data", exist_ok=True)
    dim_facilities.to_parquet("data/dim_facilities.parquet", index=False)
    dim_calendar.to_parquet("data/dim_calendar.parquet", index=False)
    fact_outages.to_parquet("data/fact_outages.parquet", index=False)
    
    print(f"Modelo completado: 3 tablas actualizadas.")

if __name__ == "__main__":
    process_data_model()
