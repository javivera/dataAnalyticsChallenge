from datetime import date
from faulthandler import disable
from tkinter import CURRENT, filedialog
import pandas as pd
import os
import csv
import psycopg2
import numpy as np
import serverLoader
import serverLoaderAlkemy
import logging
import datetime

# Configuración necesaria para que decouple detecte .env (donde están guardadas las configuraciones)
from decouple import config
from decouple import AutoConfig

config = AutoConfig(search_path=os.getcwd())

DEBUG = config('DEBUG')
DATASETS_ROOT_DIR = config('DATASETS_ROOT_DIR')

# Obtiene fecha actual
CURRENT_DATE = datetime.datetime.now().strftime("%d-%m-%Y")


def directoryLister(rootdir=DATASETS_ROOT_DIR):
    # Acá simplemente armamos una lista con todas las direcciones enteras de nuestros archivos csv
    # Decidí armarlo en una función por si a la hora de utilizarlo se quisiera cambiar la dirección raíz
    # Para cambiarla se debe modificar DATASET_ROOT_DIR en .env

    filesDirs = []

    for rootdir, dirs, files in os.walk(rootdir):
        for file in files:
            if CURRENT_DATE in file:
                filesDirs.append(os.path.join(rootdir, file))

    return filesDirs


def dfListCreator(fileDirs):
    # Lee csv y crea dataframes luego los guarda en una lista
    dataFrames = []
    for j in fileDirs:
        with open(j, 'r') as f:
            dataFrames.append(pd.read_csv(j))
        f.close()
    return dataFrames


def column_checker(df, columns):
    # Esta función está diseñada para evitar que un dataset malo corte el proceso y permitir así que
    # los datasets buenos sean procesados, con datasets malos me refiero a datasets donde los nombres de las columnas
    # no corresponden a lo esperado.
    for j in columns:
        if j not in df.columns:
            return False


# No la uso en este caso, por que no queda claro que espera Alkemy , y como solo tengo 3 datasets ,
# terminaria perdiendo mucha información
# Para evitar esto hice a mano los casos necesarios. Pero en un caso real, probablemente descartaría los datasets rotos
# y hablaría con los que proveen los datasets, para identificar la raíz del problema

# De todas maneras desde .env se puede elegir usar el checker o no.

CHECKER = config('CHECKER')
DATASETS_ROOT_DIR = config('DATASETS_ROOT_DIR')


def columnCleaner(dfList, columns, columns_alt='[]'):
    # Función de limpieza de dataframe, en este caso, normaliza los nombres de las columnas que diferían de lo esperado

    for j in range(0, len(dfList)):
        dfList[j].columns = dfList[j].columns.str.lower()
        dfList[j].columns = dfList[j].columns.str.normalize(
            'NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        if CHECKER == True:
            if column_checker(dfList[j], columns):
                dfList[j] = dfList[j][columns]

        else:
            if 'domicilio' in dfList[j].columns:
                dfList[j] = dfList[j][columns]

                dfList[j].rename(columns={'idprovincia': 'id_provincia',
                                          'iddepartamento': 'id_departamento',
                                          'categoria': 'categoria',
                                          'cp': 'codigo_postal',
                                          'telefono': 'numero_de_telefono', }, inplace=True)

            elif 'direccion' in dfList[j].columns:
                dfList[j] = dfList[j][columns_alt]

                dfList[j].rename(columns={'idprovincia': 'id_provincia',
                                          'iddepartamento': 'id_departamento',
                                          'categoria': 'categoria',
                                          'cp': 'codigo_postal',
                                          'telefono': 'numero_de_telefono',
                                          'direccion': 'domicilio'}, inplace=True)


def datosCleaner(df_list):
    # Función de limpieza y preparacion de dataframe, agrega NULL que es el tipo que usa posgreSQL

    for df in df_list:
        df.loc[df['mail'].isnull(), ['mail']] = 'NULL'
        df.loc[df['mail'] == np.nan, ['mail']] = 'NULL'
        df.loc[df['web'].isnull(), ['web']] = 'NULL'
        df.loc[df['web'] == 's/d', ['web']] = 'NULL'
        df.loc[df['cod_loc'].isnull(), ['cod_loc']] = 'NULL'
        df.loc[df['id_provincia'].isnull(), ['id_provincia']] = 'NULL'
        df.loc[df['id_departamento'].isnull(), ['id_departamento']] = 'NULL'
        df.loc[df['categoria'].isnull(), ['categoria']] = 'NULL'
        df.loc[df['nombre'].isnull(), ['nombre']] = 'NULL'
        df.loc[df['localidad'].isnull(), ['localidad']] = 'NULL'
        df.loc[df['provincia'].isnull(), ['provincia']] = 'NULL'
        df.loc[df['domicilio'].isnull(), ['domicilio']] = 'NULL'
        df.loc[df['codigo_postal'].isnull(), ['codigo_postal']] = 'NULL'
        df.loc[df['numero_de_telefono'].isnull(), ['numero_de_telefono']
               ] = 'NULL'
        df.loc[df['numero_de_telefono'] == 'nan',
               ['numero_de_telefono']] = 'NULL'

        df = df.dropna(how='all')


def dateAdder(df_list):
    # Agrega columna con fecha actual
    for df in df_list:
        df['fecha_de_carga'] = CURRENT_DATE


def cinesCleaner(df):
    # Otro limpiador , pero preparado para el dataframe de cines
    # Descarta las columnas innecesarias, despues corrije la columna espacio_INCAA que tiene muchos espacios vacíos
    # Finalmente transforma el tipo a enteros y luego cuenta cada apricion de cada columna , según la provincia

    df = df[['Provincia', 'Butacas', 'Pantallas', 'espacio_INCAA']]

    df.loc[df['espacio_INCAA'] == 'SI', ['espacio_INCAA']] = 1
    df.loc[df['espacio_INCAA'] == 'si', ['espacio_INCAA']] = 1

    df = df.dropna()

    df['espacio_INCAA'] = df['espacio_INCAA'].astype(int)
    df['Butacas'] = df['Butacas'].astype(int)
    df['Pantallas'] = df['Pantallas'].astype(int)

    df = df.groupby(['Provincia']).sum()
    df = df.reset_index(level=0)
    df.columns = map(str.lower, df.columns)

    df['fecha_de_carga'] = CURRENT_DATE
    return df


def dataPipeline():
    # Este pipeline, prepara los tres datasets limpiando las columnas no requeridas, basado en la primera consigna

    # Obtiene direcciones de archivos csv en base a la rootdir
    fileDirs = directoryLister(DATASETS_ROOT_DIR)
    # print(fileDirs)
    df_list = dfListCreator(fileDirs)

    # Procesa cada dataframe para seleccionar ciertas columnas establecidas en la configuracion .env
    columns = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria', 'provincia',
               'localidad', 'nombre', 'domicilio', 'cp', 'telefono', 'mail', 'web']
    columns_alt = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria',
                   'provincia', 'localidad', 'nombre', 'direccion', 'cp', 'telefono', 'mail', 'web']
    columnCleaner(df_list, columns, columns_alt)

    datosCleaner(df_list)
    dateAdder(df_list)

    return df_list


def cinesPipeline():
    # Este pipeline prepara el datasets de cines, para ser subido a la base de datos, basado en la tercera consigna

    cinesDir = directoryLister(DATASETS_ROOT_DIR + '/salas-de-cine')
    cinesDf = dfListCreator(cinesDir)[0]

    cinesDf = cinesCleaner(cinesDf)

    return cinesDf


def datosConjuntosPipeline(df_list):
    # Aquí creamos un ulitmo dataframe que intenta encarar la consigna dos, lamentablemente despues de pensarlo un
    # buen rato no logro entender exactamente que estan pidiendo, por lo que propuse este dataframe que es interesante
    # por que deja ver museos, cines y bibliotecas en cada provincia

    data_df = pd.concat(df_list)
    data_df = data_df[['provincia', 'categoria', 'fecha_de_carga']]
    data_df = data_df.groupby(
        ['provincia', 'categoria'], as_index=False).value_counts()
    data_df = data_df[['provincia', 'categoria', 'count', 'fecha_de_carga']]

    return data_df


def serverPipeline():
    # Esta seccíon es la que se encarga de crear tablas, ejecutra los pipelines, para crear y limpiar los dataframes
    # y luego de subir todo al servidor

    cines_df = cinesPipeline()
    df_list = dataPipeline()
    data_df = datosConjuntosPipeline(df_list)

    serverLoader.createTables()
    serverLoader.updateTablesCine(cines_df)
    serverLoader.updateTableDatos(df_list[0], 'datos_bibliotecas')
    serverLoader.updateTableDatos(df_list[1], 'datos_museos')

    # Borro las ultimas dos filas, por que hay problema al solicitar info desde la web del gobierno
    # Mi suposicion es que es un bug en el iterador del request.get, para chequear el problema deje un modo
    # debug en el dataGatherer, se puede activar cambiando la variable DEBUG el .env
    df_list[2] = df_list[2].drop(df_list[2].tail(2).index)
    serverLoader.updateTableDatos(df_list[2], 'datos_cines')

    serverLoader.updateTablesDatosConjuntos(data_df)


def cleanAndUploadPipelineAlkemy():
    # Esta seccíon es la que se encarga de crear tablas, ejecutra los pipelines, para crear y limpiar los dataframes
    # y luego de subir todo al servidor

    cines_df = cinesPipeline()
    df_list = dataPipeline()
    data_df = datosConjuntosPipeline(df_list)
    server_engine = serverLoaderAlkemy.serverConn()

    serverLoaderAlkemy.createTables(server_engine)

    serverLoaderAlkemy.serverLoad(cines_df, 'cines', server_engine)

    serverLoaderAlkemy.serverLoad(
        df_list[0], 'datos_bibliotecas', server_engine)
    serverLoaderAlkemy.serverLoad(df_list[1], 'datos_museos', server_engine)

    # Borro las ultimas dos filas, por que hay problema al solicitar info desde la web del gobierno
    # Mi suposicion es que es un bug en el iterador del request.get, para chequear el problema deje un modo
    # debug en el dataGatherer, se puede activar cambiando la variable DEBUG el .env
    df_list[2] = df_list[2].drop(df_list[2].tail(2).index)
    serverLoaderAlkemy.serverLoad(df_list[2], 'datos_cines', server_engine)
    serverLoaderAlkemy.serverLoad(data_df, 'datos_general', server_engine)
