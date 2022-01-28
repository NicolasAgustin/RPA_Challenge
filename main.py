import requests
import json
import os
import sys
import pandas as pd
import database
import process_data

from decouple import config

# TODO:
#      - Crear jerarquia de carpetas
#      - Chequear que existan todas las claves necesarias en el archivo settings
#      - Procesar la informacion de los cines dentro del modulo de procesamiento de datos
#      - Agregar un log de informacion
#      - Agregar validaciones para los archivos, las conexiones de la DB y las tablas
#      - Mejorar la jerarquia de excepciones
#      - Agregar un venv
#      - Revisar sintaxis PEP8

def make_environment():
    if not os.path.exists(r'datos'):
        os.mkdir('datos')
    # Crear la carpeta con la fecha de ejecucion

def proccess_data():
    make_environment()

    data = None 
    
    # Instanciamos la base de datos
    # pasar estos argumentos con settings
    db = database.Database(user=config('DBUSER'),
                           passwd=config('DBPSWD'),
                           host=config('DBHOST'),
                           port=config('DBPORT'),
                           db=config('DBNAME'))

    db.drop_data_from_table('datos')
    db.drop_data_from_table('cines')

    (df_dict, df_sql) = process_data.main()

    db.insert_dataframe(df_sql)

    return

    # db.create_default_tables()

    
        

def main():
    proccess_data()

if __name__ == "__main__":
    main()

