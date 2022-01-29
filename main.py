import requests
import json
import os
import sys
import re
import pandas as pd
import database
import process_data
import datetime
import shutil

from decouple import config

# TODO:
#      - Chequear que existan todas las claves necesarias en el archivo settings
#      - Agregar un log de informacion
#      - Agregar validaciones para los archivos, las conexiones de la DB y las tablas
#      - Mejorar la jerarquia de excepciones
#      - Agregar un venv
#      - Revisar sintaxis PEP8

meses = {'1': 'enero',
         '2': 'febrero',
         '3': 'marzo',
         '4': 'abril',
         '5': 'mayo',
         '6': 'junio',
         '7': 'julio',
         '8': 'agosto',
         '9': 'septiembre',
         '10': 'octubre',
         '11': 'noviembre',
         '12': 'diciembre'}

def make_environment():
    datos_path = config('DATOSPATH')
    if not os.path.exists(datos_path):
        os.mkdir(datos_path)
    for dir in ['Salas de cine', 'Bibliotecas populares', 'Museos']:
        path = '{}/{}/'.format(datos_path, dir)
        if not os.path.exists(path):
            os.mkdir(path)
        
    # Crear la carpeta con la fecha de ejecucion

def separate_input():
    dt = datetime.datetime.now()
    (dia, mes, anio) = dt.day, dt.month, dt.year
    input_path = config('INPUTPATH')
    bibliotecas = config('BIBLIOTECAS')
    museos = config('MUSEOS')
    cines = config('CINES')
    for file_input in os.listdir(input_path):
        (name, extension) = file_input.split('.')
        if not extension == 'csv':
            continue
        filepath = '{}/{}'.format(input_path, file_input)

        print('Encontrado el archivo: ' + file_input)

        # Nuevo nombre para el archivo
        new_name = name + "-{}-{}-{}".format(dia, mes, anio) + ".{}".format(extension)

        print('Nuevo nombre para el archivo ' + file_input  + ': ' + new_name)

        if re.search(".*[bB]iblioteca.*", name):
            date_folder = bibliotecas + "{}-{}/".format(dia, meses[str(mes)])

            if not os.path.exists(date_folder):
                os.mkdir(date_folder)

            shutil.copyfile(filepath, date_folder + file_input)
            os.rename(date_folder + file_input, date_folder + new_name)

        if re.search(".*[cC]ine.*", name):
            date_folder = cines + "{}-{}/".format(dia, meses[str(mes)])

            if not os.path.exists(date_folder):
                os.mkdir(date_folder)

            shutil.copyfile(filepath, date_folder + file_input)
            os.rename(date_folder + file_input, date_folder + new_name)

        if re.search(".*[mM]useo.*", name):
            date_folder = museos + "{}-{}/".format(dia, meses[str(mes)])

            if not os.path.exists(date_folder):
                os.mkdir(date_folder)

            shutil.copyfile(filepath, date_folder + file_input)
            os.rename(date_folder + file_input, date_folder + new_name)
            
def empty_input_folder():
    input_folder = config('INPUTPATH')
    for file in os.listdir(input_folder):
        os.remove(input_folder + file)


def main():

    make_environment()

    separate_input()

    # Instanciamos la base de datos
    # pasar estos argumentos con settings
    db = database.Database(user=config('DBUSER'),
                           passwd=config('DBPSWD'),
                           host=config('DBHOST'),
                           port=config('DBPORT'),
                           db=config('DBNAME'))

    # Agregar revision para chequear cuando la tabla no existe, ya que tira un error cuando la tabla no existe
    db.drop_data_from_table('datos')
    db.drop_data_from_table('cines')

    (df_cines, df_sql) = process_data.main()

    db.insert_dataframe(df_sql, 'datos')
    db.insert_dataframe(df_cines, 'cines')

    # empty_input_folder()

if __name__ == "__main__":
    main()

