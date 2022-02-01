import requests
import logging
from decouple import config

def main():

    """Modulo para descargar los archivos de input desde la pagina del gobierno
    Actualmente no funciona, debido a que cuando se intenta hacer un get a alguna de las url abajo definidas
    se redirecciona hacia otra pagina y no hacia el archivo
    """

    logging.basicConfig(filename=config('LOGPATH'),
                        format='%(asctime)s - %(message)s', 
                        level=logging.INFO)

    museos_url = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv"
    cines_url = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv"
    bibliotecas_url = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"

    input_path = config('INPUTPATH')

    try:
        r = requests.get(museos_url)
        open(input_path + 'museo.csv', 'wb').write(r.content)
    except Exception:
        logging.info("No se pudo descargar el dataset de museos.")

    try:
        r = requests.get(cines_url)
        open(input_path + 'cine.csv', 'wb').write(r.content)
    except Exception:
        logging.info("No se pudo descargar el dataset de cines.")

    try:
        r = requests.get(bibliotecas_url)
        open(input_path + 'biblioteca_popular.csv', 'wb').write(r.content)
    except Exception:
        logging.info("No se pudo descargar el dataset de bibliotecas")

if __name__ == '__main__':
    main()