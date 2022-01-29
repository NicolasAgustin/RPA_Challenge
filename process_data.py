import pandas as pd
import pandasql as ps
import os
import numpy as np
import re
import unidecode
from decouple import config
from pandasql import sqldf

def normalize_headers(headers):
    for i in range(len(headers)):
        headers[i] = headers[i].lower()
        headers[i] = unidecode.unidecode(headers[i])
        if headers[i] == "direccion":
            headers[i] = "domicilio"
    return headers

def create_split_list(actual_headers):
    spliting_list = []
    headers_needed = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 
                      'nombre', 'domicilio', 'cp', 'telefono', 'mail', 'web']

    for i in range(len(headers_needed)):
        if headers_needed[i] in actual_headers:
            spliting_list.append(headers_needed[i])

    return spliting_list
    
def main():
    input_path = config('INPUTPATH')

    dataframes = {}
    accum = []

    for file_input in os.listdir(input_path):

        print("Procesando archivo: " + file_input)

        data = pd.read_csv(input_path + file_input)

        # Cambiamos los campos por None para que en la BD se visualicen como NULL
        data = data.applymap(lambda x: x if not x in ['s/d', ''] else None)

        # Normalizamos los encabezados y se los seteamos al dataframe
        data.columns = normalize_headers(data.columns.tolist())

        # Creamos la que va a ser la clave para acceder al dataframe
        key = file_input.split('.')[0]
        if re.search(".*[mM]useo.*", key):
            key = "museo"
        elif re.search('.*[cC]ine.*', key):
            key = "cine"
        elif re.search('.*[bB]iblioteca.*', key):
            key = "biblioteca"

        dataframes.update({key: data})

        sptlist = create_split_list(data.columns.tolist())

        aux = data[sptlist]
        accum.append(aux)

    data = pd.concat(accum, join="outer")
    # Eliminamos los campos que son nan
    data = data.astype(object).where(pd.notnull(data), None)

    df_cines = dataframes['cine']

    query = """SELECT provincia, SUM(pantallas) as pantallas, COUNT(espacio_incaa) as espacio_incaa, SUM(butacas) as butacas
            FROM df_cines GROUP BY provincia"""

    cines = sqldf(query, locals())

    print("Informacion procesada del dataframe")
    print(cines)

    return (cines, data)
        
if __name__ == '__main__':
    main()
