from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Datos(Base):
    __tablename__   = 'datos'
    id              = Column(Integer, primary_key=True)
    cod_localidad   = Column(Integer)
    id_provincia    = Column(Integer)
    id_departamento = Column(Integer)
    categoria       = Column(String(255))
    provincia       = Column(String(255))
    localidad       = Column(String(255))
    nombre          = Column(String(255))
    domicilio       = Column(String(255))
    codigo_postal   = Column(String(255))
    telefono        = Column(String(255))
    mail            = Column(String(255))
    web             = Column(String(255))

class Cines(Base):
    __tablename__       = 'cines'
    id                  = Column(Integer, primary_key=True)
    provincia           = Column(String(255))
    cant_pantallas      = Column(Integer)
    cant_butacas        = Column(Integer)
    cant_espacios_incaa = Column(Integer)