from curses import echo
import datetime
import traceback
from psycopg2 import connect
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker

from sqlalchemy_utils import database_exists, create_database

from models import Datos

class Database:

    def __init__(self, user, passwd, host, port, db):
        url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        self.engine = create_engine(url, echo=False)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def test(self):
        registro = Datos(id=1,
                         cod_localidad=12,
                         id_provincia=12,
                         id_departamento=12,
                         categoria='cine',
                         provincia='santa fe',
                         localidad='rosario',
                         nombre='hoyts',
                         domicilio='calle 123',
                         codigo_postal='2000',
                         telefono='34213412',
                         mail=None,
                         web=None)
        try:
            self.session.add(registro)
            self.session.commit()
        finally:
            if self.session is not None:
                self.session.close()

    def create_default_tables(self):
        try:
            connection = self.engine.connect()
            connection.execute(open("scripts/create_data_table.sql","r").read())
            connection.execute(open("scripts/create_cines_table.sql","r").read())
            self.session.commit()
        except Exception as ex:
            self.log_error()
            
    def log_error(self):
        logger = logging.getLogger('log')
        logger.setLevel(logging.ERROR)
        fh = logging.FileHandler('error.log')
        fh.setLevel(logging.ERROR)
        logger.addHandler(fh)
        timestamp = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
        logger.error(timestamp + ' %s', exc_info=1)

    def drop_data_from_table(self, table):
        try:
            sql = "TRUNCATE %s" % table
            self.engine.connect()
            self.engine.execute(sql)
            self.session.commit()
        except Exception:
            print('Error eliminando la informacion')
            self.log_error()

    def insert_dataframe(self, dataframe):
        try:
            connection = self.engine.connect()
            dataframe.to_sql('datos', connection, if_exists='replace', index = False)
            self.session.commit()
        except Exception as ex:
            print("Error insertando registro")
            self.log_error()

if __name__ == '__main__':
    pass