# REST API para carga y respaldo de archivos CSV

Esta API permite:

- Subir archivos CSV por tabla (`/upload/{table_name}`)
- Validar columnas y tipos según un diccionario de datos
- Insertar hasta 1000 filas por solicitud
- Hacer backups en formato AVRO (`/backup/{table_name}`)
- Restaurar desde el último backup (`/restore/{table_name}`)

## Requisitos para el programa

```bash
pip install -r requirements.txt