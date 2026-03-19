import pytest
import requests_mock
import pandas as pd
from src.connector import EIAConnector

def test_invalid_api_key():
    """Prueba que el conector maneje correctamente una API Key inválida (403)"""
    connector = EIAConnector()
    with requests_mock.Mocker() as m:
        # Simulamos que la EIA responde 403 Forbidden
        m.get(connector.base_url, status_code=403)
        df = connector.fetch_data()
        assert df is None  # El script debe devolver None según nuestra lógica

def test_data_validation_fail():
    """Prueba que el guardado falle si faltan columnas requeridas"""
    connector = EIAConnector()
    # Creamos un DataFrame al que le falta la columna obligatoria 'outage'
    df_invalid = pd.DataFrame([{'period': '2024-01-01', 'facilityName': 'Test Plant'}])
    
    # Intentamos guardar y verificamos que no truene el programa (manejo de errores)
    try:
        connector.save_data(df_invalid)
        pytest.raises = True
    except Exception:
        pytest.raises = False
    assert pytest.raises is True # El script debe capturar el error en el log, no colapsar
