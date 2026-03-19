from fastapi import FastAPI, BackgroundTasks, Security, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from src.connector import EIAConnector
from src.models import process_data_model
import duckdb
import os
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURACIÓN DE SEGURIDAD ---
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
APP_AUTH_KEY = os.getenv("APP_AUTH_KEY", "default_secret")

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == APP_AUTH_KEY:
        return api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No autorizado: X-API-KEY inválida o ausente"
    )

app = FastAPI(title="Nuclear Outages API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

connector = EIAConnector()

def run_pipeline():
    df = connector.fetch_data()
    if df is not None and not df.empty:
        connector.save_data(df)
        process_data_model()

@app.get("/")
def home():
    return {"message": "API Running", "auth_required_on": ["/refresh"]}

@app.get("/data")
def get_data(limit: int = 100, offset: int = 0, facility: str = None):
    path = "data/fact_outages.parquet"
    if not os.path.exists(path):
        return {"error": "No hay datos disponibles. Use /refresh."}

    # CONSTRUCCIÓN DE QUERY (CORREGIDA)
    query = f"SELECT * FROM '{path}' WHERE 1=1"
    
    if facility:
        # Usamos ILIKE para búsqueda parcial e insensible a mayúsculas
        query += f" AND facilityName ILIKE '%{facility}%'"
    
    # Ordenar por fecha descendente para ver lo más reciente primero
    query += f" ORDER BY period DESC LIMIT {limit} OFFSET {offset}"
    
    data = duckdb.query(query).df().to_dict(orient="records")
    return {"count": len(data), "data": data}

@app.post("/refresh")
async def refresh_data(background_tasks: BackgroundTasks, auth: str = Security(get_api_key)):
    background_tasks.add_task(run_pipeline)
    return {"message": "Actualización iniciada correctamente."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
