import os
import requests
import pandas as pd
from dotenv import load_dotenv
import time
import logging
from deltalake.writer import write_deltalake
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

class EIAConnector:
    def __init__(self):
        self.api_key = os.getenv("EIA_API_KEY")
        if not self.api_key:
            raise ValueError("CRÍTICO: No se encontró la EIA_API_KEY.")
        
        self.base_url = "https://api.eia.gov/v2/nuclear-outages/generator-nuclear-outages/data/"
        self.delta_path = "data/nuclear_outages_delta"

    def get_last_period(self):
        try:
            dt = DeltaTable(self.delta_path)
            last_period = dt.to_pandas(columns=['period'])['period'].max()
            logging.info(f"Último dato: {last_period}. Extracción incremental...")
            return last_period
        except:
            logging.info("Sin tabla previa. Extracción completa...")
            return None

    def fetch_data(self, limit=5000):
        all_data = []
        offset = 0
        last_period = self.get_last_period()
        
        while True:
            params = {
                "api_key": self.api_key,
                "frequency": "daily",
                "data[]": "outage",
                "offset": offset,
                "length": limit,
            }

            # En la v2, si usas 'start', no uses offset alto o te dará error 400
            if last_period:
                params["start"] = last_period

            for attempt in range(2):
                try:
                    response = requests.get(self.base_url, params=params, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    break
                except Exception as e:
                    if attempt == 0:
                        time.sleep(2)
                    else:
                        logging.error(f"Fallo crítico: {e}")
                        return None

            rows = data.get('response', {}).get('data', [])
            if not rows:
                break
                
            all_data.extend(rows)
            logging.info(f"Recuperados: {len(all_data)}")
            
            offset += limit
            # Si trajimos menos del límite o el total ya se alcanzó, paramos
            total = int(data.get('response', {}).get('total', 0))
            if len(rows) < limit or offset >= total:
                break

        df = pd.DataFrame(all_data)
        if not df.empty:
            subset_cols = ['period', 'facilityName', 'unitName'] 
            df = df.drop_duplicates(subset=[c for c in subset_cols if c in df.columns])
        
        return df

    def save_data(self, df):
        if df.empty:
            return
        os.makedirs("data", exist_ok=True)
        try:
            # Importante: Usamos overwrite si es la primera vez o si hay cambio de esquema
            mode = "append" if os.path.exists(self.delta_path) else "overwrite"
            write_deltalake(self.delta_path, df, mode=mode)
            logging.info(f"Delta Table actualizada en {self.delta_path}")
        except Exception as e:
            logging.warning(f"Error en save: {e}")
            try:
                write_deltalake(self.delta_path, df, mode="overwrite")
            except Exception as e2:
                logging.error(f"Error crítico en save: {e2}")

if __name__ == "__main__":
    connector = EIAConnector()
    df = connector.fetch_data()
    connector.save_data(df)
