from fastapi.testclient import TestClient
from src.api import app

client = TestClient(app)

def test_read_main():
    """Prueba que la raíz de la API responda correctamente"""
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()

def test_get_data_structure():
    """Prueba que el endpoint /data devuelva la estructura JSON esperada"""
    # Nota: esto asume que ya ejecutaste el pipeline una vez y hay datos
    response = client.get("/data?limit=1")
    if response.status_code == 200:
        json_data = response.json()
        assert "data" in json_data
        assert "count" in json_data
    else:
        # Si no hay datos, al menos debe dar un error controlado (404 o mensaje)
        assert response.json()["error"] is not None

def test_data_limit_filter():
    """Prueba que el parámetro 'limit' realmente limite los resultados"""
    limit = 5
    response = client.get(f"/data?limit={limit}")
    if response.status_code == 200:
        assert len(response.json()["data"]) <= limit
