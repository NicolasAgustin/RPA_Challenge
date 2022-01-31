# RPA_Challenge
Challenge Base Python Calyx

## Creacion del entorno virtual

Instalamos el paquete virtualenv: \
``` pip install virtualenv ``` \
Creamos el entorno virtual:\
``` python -m virtualenv (name) ```\
Activamos el entorno:\
``` (name)\Scripts\activate.bat ```\
Ahora procedemos a instalar las dependencias: \
``` pip install -r requeriments.txt ```

## Archivos de configuracion

Antes de la ejecucion se debe crear un archivo de configuracion ```settings.ini``` con las siguientes entradas:

> * LOGPATH: Path al archivo de log general 
> * LOGERRORPATH: Path al archivo de log de errores
> * INPUTPATH: Path al directorio de input
> * DATOSPATH: Path al directorio de datos (output)
> * BIBLIOTECAS: Path al directorio de datos de bibliotecas
> * MUSEOS: Path al directorio de datos de museos
> * CINES: Path al directorio de datos de cines
> * DBUSER: Usuario de la base de datos
> * DBPSWD: Password de la base de datos
> * DBHOST: Host de la base de datos
> * DBPORT: Puerto de la base de datos
> * DBNAME: Nombre de la base de datos

# Bonus Track

Para poder hacer deploy del proyecto se debe contar con Selenium y el [driver para chrome](https://chromedriver.chromium.org). El archivo .exe se debe colocar el la carpeta raiz del proyecto (bonus_track)

## Arhivo de configuracion

Se debe crear un archivo ```settings.ini``` con las siguientes entradas:
> * OUTPUTPATH: Path al directorio de output
> * URL: Url a la pagina de BNA 

