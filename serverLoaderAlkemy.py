import logging
import sqlalchemy
from decouple import config

USERNAME = config('USERNAME')
PASSWORD = config('PASSWORD')
HOST = config('HOST')
DBNAME = config('DBNAME')


def serverConn():
    try:
        server_engine = sqlalchemy.create_engine(
            "postgresql://{}:{}@{}/{}".format(USERNAME, PASSWORD, HOST, DBNAME))

        logging.debug('La conexión al servidor fue exitosa')
        logging.debug(server_engine)
    except Exception as e:
        logging.debug('Hubo un problema al conectar con el servidor')
        logging.debug(SystemExit(e))
        raise 

    return server_engine


def createTables(server_engine):
    try:
        with open('sqlScripts/creadorTablas.sql') as f:
            for line in f:
                server_engine.execute(line)

        logging.debug('Tablas creadas exitosamente')

    except Exception as e:
        logging.debug('Hubo un problema al crear las tablas')
        logging.debug(SystemExit(e))
        # En el log puse solo el error final, me pareció mas prolijo, el error completo queda para la consola
        raise


def serverLoad(df, table_name, server_engine):

    conn = server_engine.connect()

    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        logging.debug('La tabla {} fue subido con éxito a la base de datos {}'.format(
            table_name, DBNAME))
    except Exception as e:
        logging.debug('Hubo un error al subir datos al servidor')
        logging.debug(SystemExit(e))

    conn.close()
