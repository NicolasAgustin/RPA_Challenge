import logging
import os
import re
import database
import process_data
import datetime
import shutil

from decouple import config

# TODO:
#      - Chequear que existan todas las claves necesarias en el archivo settings
#      - Agregar validaciones para las conexiones de la DB
#      - Revisar sintaxis PEP8
#      - Cuando convertimos del DataFrame a sql los registros se suben sin clave primaria

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
    try:
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

                if os.path.isfile(date_folder + new_name):
                    os.remove(date_folder + new_name)

                shutil.copyfile(filepath, date_folder + file_input)
                os.rename(date_folder + file_input, date_folder + new_name)

            if re.search(".*[cC]ine.*", name):
                date_folder = cines + "{}-{}/".format(dia, meses[str(mes)])

                if not os.path.exists(date_folder):
                    os.mkdir(date_folder)

                if os.path.isfile(date_folder + new_name):
                    os.remove(date_folder + new_name)

                shutil.copyfile(filepath, date_folder + file_input)
                os.rename(date_folder + file_input, date_folder + new_name)

            if re.search(".*[mM]useo.*", name):
                date_folder = museos + "{}-{}/".format(dia, meses[str(mes)])

                if not os.path.exists(date_folder):
                    os.mkdir(date_folder)

                if os.path.isfile(date_folder + new_name):
                    os.remove(date_folder + new_name)

                shutil.copyfile(filepath, date_folder + file_input)
                os.rename(date_folder + file_input, date_folder + new_name)
        
        return True
    except Exception as ex:
        log_error()
        return False

def log_error():
    logger = logging.getLogger('error')
    logger.setLevel(logging.ERROR)
    fh = logging.FileHandler('error.log')
    fh.setLevel(logging.ERROR)
    logger.addHandler(fh)
    timestamp = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
    logger.error(timestamp + ' %s', exc_info=1)

def empty_input_folder():
    input_folder = config('INPUTPATH')
    for file in os.listdir(input_folder):
        os.remove(input_folder + file)

def main():

    print("Antes de la creacion del log")

    logging.basicConfig(filename=config('LOGPATH'),
                        format='%(asctime)s - %(message)s', 
                        level=logging.INFO)

    print("Antes del bucle")

    for setting in ["LOGPATH", "LOGERRORPATH", "INPUTPATH", "DATOSPATH", "BIBLIOTECAS", 
                     "MUSEOS", "CINES", "DBUSER", "DBPSWD", "DBHOST", "DBPORT", "DBNAME"]:
        if config(setting) == "":
            logging.info('ERROR: No existe la clave {} en el archivo de configuracion.'.format(setting))
            return

    make_environment()

    logging.info('Make environment: OK')

    # Aca falta descargar el input con requests

    if not separate_input():
        logging.info('Separate input: ERROR')
        log_error()
        return 

    logging.info('Separate input: OK')

    try:
        # Instanciamos la base de datos
        # pasar estos argumentos con settings
        db = database.Database(user=config('DBUSER'),
                            passwd=config('DBPSWD'),
                            host=config('DBHOST'),
                            port=config('DBPORT'),
                            db=config('DBNAME'))
    except:
        logging.info('Database: ERROR')
        log_error()
        return

    logging.info('Database: OK')

    try:
        # Agregar revision para chequear cuando la tabla no existe, ya que tira un error cuando la tabla no existe
        db.drop_data_from_table('datos')
        db.drop_data_from_table('cines')
    except:
        logging.info('Drop data from tables: ERROR')
        log_error()
        return

    logging.info('Drop data from tables: OK')

    try:
        (df_cines, df_sql) = process_data.main()
    except:
        logging.info('Process data: ERROR')
        log_error()
        return

    logging.info('Process data: OK')

    try:
        db.insert_dataframe(df_sql, 'datos')
        db.insert_dataframe(df_cines, 'cines')
    except:
        logging.info('Push data to DB: ERROR')
        log_error()
        return

    logging.info('Push data to DB: OK')

    empty_input_folder()

    logging.info('Clear input folder: OK')

if __name__ == "__main__":
    main()

