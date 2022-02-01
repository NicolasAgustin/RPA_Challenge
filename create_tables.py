import logging
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

def main():
    """ Crea las tablas por defecto que deberia tener la base de datos
    """
    try:
        user = config('DBUSER')
        passwd = config('DBPSWD')
        host = config('DBHOST')
        port = config('DBPORT')
        db=config('DBNAME')
        url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        engine = create_engine(url, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        connection = engine.connect()
        connection.execute(open("scripts/create_data_table.sql","r").read())
        connection.execute(open("scripts/create_cines_table.sql","r").read())
        session.commit()
    except Exception:
        logger = logging.getLogger('error')
        logger.setLevel(logging.ERROR)
        fh = logging.FileHandler(config('LOGERRORPATH'))
        fh.setLevel(logging.ERROR)
        logger.addHandler(fh)
        timestamp = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
        logger.error(timestamp + ' %s', exc_info=1)
    finally:
        if not connection is None:
            connection.close()

if __name__ == '__main__':
    main()