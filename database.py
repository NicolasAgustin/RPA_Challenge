from curses import echo
import datetime
from psycopg2 import connect
import logging
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker

class Database:

    def __init__(self, user, passwd, host, port, db):
        url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        self.engine = create_engine(url, echo=False)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def create_default_tables(self):
        try:
            connection = self.engine.connect()
            connection.execute(open("scripts/create_data_table.sql","r").read())
            connection.execute(open("scripts/create_cines_table.sql","r").read())
            self.session.commit()
        except Exception as ex:
            self.log_error()
            
    def log_error(self):
        logger = logging.getLogger('error')
        logger.setLevel(logging.ERROR)
        fh = logging.FileHandler('error.log')
        fh.setLevel(logging.ERROR)
        logger.addHandler(fh)
        timestamp = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
        logger.error(timestamp + ' %s', exc_info=1)

    def drop_data_from_table(self, table):
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

    def insert_dataframe(self, dataframe, table_name):
        try:
            connection = self.engine.connect()
            dataframe.to_sql(table_name, connection, if_exists='replace', index = False)
            self.session.commit()
        except Exception as ex:
            print("Error insertando registro")
            self.log_error()

if __name__ == '__main__':
    pass