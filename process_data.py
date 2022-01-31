import pandas as pd
import os
import re
import unidecode
from decouple import config
from pandasql import sqldf
from datetime import datetime

def normalize_headers(headers):
    """[Normaliza los headers, debido a que aveces vienen con caracteres raros o con diferentes nombres]

    Args:
        headers ([list]): [Lista con los headers]

    Returns:
        [list]: [Lista con los headers normalizados]
    """
    for i in range(len(headers)):
        headers[i] = headers[i].lower()
        headers[i] = unidecode.unidecode(headers[i])
        if headers[i] == "direccion":
            headers[i] = "domicilio"
    return headers

def create_split_list(actual_headers):
    """[summary]

    Args:
        actual_headers ([type]): [description]

    Returns:
        [type]: [description]
    """
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

    rows = len(data.index) 
    date = datetime.now().strftime('%d/%m/%Y')
    date_column = [date] * rows
    data['fecha'] = date_column

    df_cines = dataframes['cine']

    query = """SELECT provincia, SUM(pantallas) as pantallas, COUNT(espacio_incaa) as espacio_incaa, SUM(butacas) as butacas
            FROM df_cines GROUP BY provincia"""

    try:
        cines = sqldf(query, locals())
        rows = len(cines.index) 
        cines['fecha'] = [date] * rows
    except:
        raise Exception('No se pudo ejecutar la query sobre el dataframe.')

    return (cines, data)
        
if __name__ == '__main__':
    main()
