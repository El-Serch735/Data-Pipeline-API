# Software Engineer - Technical Challenge Arkham 

## Description
Data pipeline that extracts Nuclear Outages data from the EIA API, stores it efficiently, 
and provides basic analytics and query capabilities.

Tecnologás: 
FastAPI
Pandas + PyArrow
Streamlit frontend
Parquet, DuckDB
python-dotenv

## Quick start
How to clone and start

## Dependencies Installation
mkdir nuclear-outages-challenge
cd nuclear-outages-challenge
python -m venv venv
Activate on Windows: venv\Scripts\activate | En Mac/Linux: source venv/bin/activate
pip install -r requirements.txt

## API key setup
.env instructions 
This file must have EIA_API_KEY=key

streamlit run frontend/app_frontend.py         
python -m src.api

## Assumtions made

## Data Model
Table dim_facilities {
  facilityName varchar [primary key]
  facility varchar
  generator varchar
}

Table dim_calendar {
  period datetime [primary key]
  year int
  month int
  day int
  quarter int
  month_name varchar
}

Table fact_outages {
  period datetime
  facilityName varchar
  outage float
  outage_units varchar
}

Ref: fact_outages.facilityName > dim_facilities.facilityName
Ref: fact_outages.period > dim_calendar.period

![alt text](ER.png)

## API Reference
Mini guía de tus endpoints /data y /refresh. (borrar)
/data 
/refresh 

## Result examples
## Pruebas Unitarias e Integración
Se implementaron pruebas automáticas para validar la robustez del pipeline.
python -m pytest

================================================================================ test session starts ================================================================================ 
platform win32 -- Python 3.13.0, pytest-9.0.2, pluggy-1.6.0
rootdir: C:\Users\sergi\OneDrive\Escritorio\Reto Arkham
plugins: anyio-4.12.1, requests-mock-1.12.1
collected 5 items                                                                                                                                                                     

tests\test_api.py ...                                                                                                                                                          [ 60%] 
tests\test_connector.py ..                                                                                                                                                     [100%] 

================================================================================= 5 passed in 1.48s ================================================================================= 

Puntos validados:
Manejo de errores 403 (API Key inválida).
Validación de esquema de datos (campos obligatorios).
Disponibilidad de endpoints /data y /refresh.
Formato de respuesta JSON y filtros.