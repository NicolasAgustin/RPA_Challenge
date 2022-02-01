from curses import echo
import datetime
from pandas import DataFrame
from psycopg2 import connect
import logging
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker

class Database:

    def __init__(self, user: str, passwd: str, 
                 host: str, port: str, db: str):
        """Constructor de clase

        Args:
            user (string): Usuario
            passwd (string): Contrasena
            host (string): Host donde se encuentra la DB (localhost)
            port (string): Puerto donde esta escuchando el motor de DB, por defecto para postgresql: 5432
            db (string): Nombre de la base de datos
        """
        url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        self.engine = create_engine(url, echo=False)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def create_default_tables(self):
        """Crea las tablas por defecto en la base de datos (utilizar el script create_tables.py)
        """
        try:
            connection = self.engine.connect()
            connection.execute(open("scripts/create_data_table.sql","r").read())
            connection.execute(open("scripts/create_cines_table.sql","r").read())
            self.session.commit()
        except Exception:
            self.log_error()
            
    def log_error(self):
        """Logea un error
        """
        logger = logging.getLogger('error')
        logger.setLevel(logging.ERROR)
        fh = logging.FileHandler('error.log')
        fh.setLevel(logging.ERROR)
        logger.addHandler(fh)
        timestamp = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
        logger.error(timestamp + ' %s', exc_info=1)

    def drop_data_from_table(self, table: str):
        """Elimina la informacion presente en una tabla de la base de datos

        Args:
            table (string): Nombre de la tabla
        """
        try:
            ins = sqlalchemy.inspect(self.engine)
            if ins.has_table(table):
                sql = "TRUNCATE %s" % table
                self.engine.connect()
                self.engine.execute(sql)
                self.session.commit()
        except Exception:
            print('Error eliminando la informacion')
            self.log_error()

    def insert_dataframe(self, dataframe: DataFrame, table_name: str):
        """Inserta en la base de datos un DataFrame de pandas

        Args:
            dataframe (DataFrame): Tabla de datos
            table_name (string): Nombre de la tabla a la que se va a insertar
        """
        try:
            connection = self.engine.connect()
            dataframe.to_sql(table_name, connection, if_exists='replace', index = False)
            self.session.commit()
        except Exception:
            print("Error insertando registro")
            self.log_error()

if __name__ == '__main__':
    pass