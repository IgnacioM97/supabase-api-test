from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, HTTPException
import os
import csv
import shutil
import datetime
from typing import Dict, List
from fastavro import writer, parse_schema, reader
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

app = FastAPI()

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

BACKUP_DIR = "backups"
os.makedirs(BACKUP_DIR, exist_ok=True)


SUPABASE_URL = os.getenv("URL_SUPABASE")
engine = create_async_engine(SUPABASE_URL, echo=True)

# Diccionario

data_schemas = {
    "hired_employees": {
        "id": "integer",
        "name": "string",
        "datetime": "string",
        "department_id": "integer",
        "job_id": "integer",
    },
    "departments": {
        "id": "integer",
        "user_id": "integer"
    },
    "jobs": {
        "id": "integer",
        "location": "string"
    }
}


# Función de validación de datos

def validacion(value: str, expected_type: str):
    if expected_type == "string":
        return str(value)
    elif expected_type == "integer":
        return int(value)
    else:
        raise ValueError(f"Tipo no soportado: {expected_type}")

def validar_csv(file_path: str, schema: Dict[str, str]) -> List[Dict]:
    data = []
    with open(file_path, newline='', encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        headers = list(schema.keys())
        
        for i, row in enumerate(reader, start=1):
        
            if len(row) != len(headers):
                raise ValueError(f"Fila {i}: número incorrecto de columnas")

            validated_row = {}
            for col_index, (col, col_type) in enumerate(schema.items()):
                try:
                    validated_row[col] = validacion(row[col_index], col_type)
                except Exception:
                    raise ValueError(f"Error en fila {i}, columna '{col}': se esperaba {col_type}")
            data.append(validated_row)
    
    return data

# Función para insertar datos a Postgres en Supabase

async def insert_to_supabase(table_name: str, rows: List[Dict]):

    async with engine.begin() as conn:
        for row in rows:
            keys = ', '.join(row.keys())
            values_placeholders = ', '.join([f":{k}" for k in row.keys()])
            sql = text(f"INSERT INTO {table_name} ({keys}) VALUES ({values_placeholders})")
            await conn.execute(sql, row)

# Función upload

@app.post("/upload/{table_name}")
async def upload_csv(table_name: str, file: UploadFile = File(...)):
    if table_name not in data_schemas:
        raise HTTPException(status_code=400, detail="Tabla no reconocida.")

    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        data = validar_csv(file_path, data_schemas[table_name])
        if not (1 <= len(data) <= 1000):
            raise ValueError("Número de filas debe estar entre 1 y 1000.")
        
        await insert_to_supabase(table_name, data)


    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))


    return {"message": f"{len(data)} filas validadas e insertadas para {table_name}"}


# Función backup

@app.post("/backup/{table_name}")
def backup_table(table_name: str):
    if table_name not in data_schemas:
        raise HTTPException(status_code=400, detail="Tabla no reconocida.")

    dummy_data = [
        {col: f"valor_{i}_{col}" if data_schemas[table_name][col] == 'string' else i for col in data_schemas[table_name]}
        for i in range(5)
    ]

    schema = {
        "doc": f"Backup de {table_name}",
        "name": table_name,
        "namespace": "backup",
        "type": "record",
        "fields": [{"name": col, "type": "string"} for col in data_schemas[table_name]]
    }
    parsed_schema = parse_schema(schema)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(BACKUP_DIR, f"{table_name}_{timestamp}.avro")
    with open(backup_file, 'wb') as out:
        writer(out, parsed_schema, dummy_data)

    return {"message": f"Backup realizado: {backup_file}"}


# Función restore

@app.post("/restore/{table_name}")
def restore_table(table_name: str):
    if table_name not in data_schemas:
        raise HTTPException(status_code=400, detail="Tabla no reconocida.")

    files = sorted([f for f in os.listdir(BACKUP_DIR) if f.startswith(table_name) and f.endswith('.avro')])
    if not files:
        raise HTTPException(status_code=404, detail="No hay backups disponibles.")

    latest_backup = os.path.join(BACKUP_DIR, files[-1])
    with open(latest_backup, 'rb') as fo:
        restored_data = list(reader(fo))

    return {"message": f"Restaurado {len(restored_data)} registros desde {latest_backup}"}



