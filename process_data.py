import pandas as pd
import os
import re
import unidecode
from decouple import config
from pandasql import sqldf
from datetime import datetime

def normalize_headers(headers: list):
    """Normaliza los headers, debido a que aveces vienen con caracteres raros o con diferentes nombres

    Args:
        headers (list): Lista con los headers

    Returns:
        list: Lista con los headers normalizados
    """
    for i in range(len(headers)):
        headers[i] = headers[i].lower()
        headers[i] = unidecode.unidecode(headers[i])
        if headers[i] == "direccion":
            headers[i] = "domicilio"
    return headers

def create_split_list(actual_headers: list):
    """Creamos una lista que servira de "rango" para recortar el dataframe. Nos aseguramos de que si el dataframe no posee un header
    que es necesario, entonces no se agregue a la lista de recorte.

    Args:
        actual_headers (list): Headers que posee el dataframe

    Returns:
        : Lista con los headers a recortar
    """
    spliting_list = []
    headers_needed = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia', 'localidad', 
                      'nombre', 'domicilio', 'cp', 'telefono', 'mail', 'web']

    for i in range(len(headers_needed)):
        if headers_needed[i] in actual_headers:
            spliting_list.append(headers_needed[i])

    return spliting_list
    
def main():
    try:
        input_path = config('INPUTPATH')
    except:
        raise Exception('Error al leer el archivo de configuracion')

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

        # sptlist va a ser el "rango" que se va a cortar del dataframe
        sptlist = create_split_list(data.columns.tolist())

        aux = data[sptlist]
        accum.append(aux)

    # Unificamos toda la informacion acumulada
    data = pd.concat(accum, join="outer")

    # Eliminamos los campos que son nan
    data = data.astype(object).where(pd.notnull(data), None)

    rows = len(data.index) 
    date = datetime.now().strftime('%d/%m/%Y')
    # Creamos una lista para completar la columna fecha en la DB
    date_column = [date] * rows
    data['fecha'] = date_column

    df = dataframes['cine']

    query = """SELECT provincia, SUM(pantallas) as pantallas, COUNT(espacio_incaa) as espacio_incaa, SUM(butacas) as butacas
            FROM df GROUP BY provincia"""

    try:

        # Ejecutamos la query sobre df
        cines = sqldf(query, locals())
        rows = len(cines.index) 
        cines['fecha'] = [date] * rows

        registros = []

        cantidad_registros_df = [
                dataframes['cine'], 
                dataframes['museo'],
                dataframes['biblioteca']
            ]

        # Unificamos la informacion obtenida
        all_data = pd.concat(cantidad_registros_df, join="outer")

        querys = [
            "SELECT fuente as tipo_registro, COUNT(*) as cantidad_registros FROM all_data GROUP BY tipo_registro",
            "SELECT categoria as tipo_registro, COUNT(*) as cantidad_registros FROM all_data GROUP BY tipo_registro",
            "SELECT provincia as tipo_registro, COUNT(*) as cantidad_registros FROM all_data GROUP BY tipo_registro"
        ]

        # Ejecutamos cada query y agregamos el dataframe obtenido a la lista de registros
        for q in querys:
            aux_df = sqldf(q, locals())
            registros.append(aux_df)
            
        # Unificamos la informacion obtenida
        cantidad_registros_df = pd.concat(registros, join="outer")

        rows = len(cantidad_registros_df.index)
        cantidad_registros_df['fecha'] = [date] * rows

    except:
        raise Exception('No se pudo ejecutar la query sobre el dataframe.')

    return (cines, data, cantidad_registros_df)
        
if __name__ == '__main__':
    main()
