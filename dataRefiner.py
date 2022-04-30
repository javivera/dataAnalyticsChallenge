from distutils.log import debug
import pandas as pd
import os
import csv
import psycopg2
import numpy as np
import serverLoader

# Configuración necesaria para que decouple detecte .env (donde están guardadas las configuraciones)
from decouple import config
from decouple import AutoConfig

config = AutoConfig(search_path=os.getcwd())

DEBUG = config('DEBUG')

# Obtiene fecha actual 
from datetime import date

today = date.today()
actual_date = today.strftime("%d/%m/%Y")

# Acá simplemente armamos una lista con todas las direcciones enteras de nuestros archivos csv
# Decidí armarlo en una función por si a la hora de utilizarlo se quisiera cambiar la dirección raíz
# Como predeterminado deje datasets que suele ser una dirección raíz común

def directoryLister(rootdir = 'datasets/'):
    filesDirs = []
    
    for rootdir, dirs, files in os.walk(rootdir):
        for file in files:
            filesDirs.append(os.path.join(rootdir, file))
    if debug == True:   
        print(filesDirs)

    return filesDirs



def dfListCreator(fileDirs):
    # Lee csv y crea dataframes luego los guarda en una lista
    dataFrames = []
    for j in fileDirs:
        with open(j, 'r') as f:
            dataFrames.append(pd.read_csv(j))
        f.close()
    return dataFrames
    

def column_checker(df,columns):
    # Esta función esta explicada abajo
    for j in columns:
        if j not in df.columns:
            
            return False

# La función column_checker está diseñada para evitar que un dataset malo corte el proceso y permitir así que los datasets buenos sean procesados
# No la uso en este caso, por que no queda claro que espera Alkemy , y como solo tengo 3 datasets , terminaria perdiendo mucha información
# Para evitar esto hice a mano los casos necesarios. Pero en un caso real, probablemente descartaría los datasets rotos, especialmente si fueran muchos
# Dependiendo siempre de si puedo o no prescindir de la información que estos brindan
# De todas maneras desde .env se puede elegir usar el checker o no.

# Además tenemos dos funciones que se encargan de limpiar y preparar los dataframes

CHECKER = config('CHECKER')

def columnCleaner(dfList,columns, columns_alt = '[]'):
    for j in range(0,len(dfList)):
        dfList[j].columns = dfList[j].columns.str.lower()
        dfList[j].columns = dfList[j].columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        if CHECKER == True:
            if column_checker(dfList[j],columns):
                dfList[j] = dfList[j][columns]

        else:
            if 'domicilio' in dfList[j].columns:
                dfList[j] = dfList[j][columns]

                dfList[j].rename(columns = {'idprovincia':'id_provincia',
                                            'iddepartamento': 'id_departamento',
                                            'categoria':'categoria',
                                            'cp':'codigo_postal',
                                            'telefono':'numero_de_telefono',}, inplace = True) 

            elif 'direccion' in dfList[j].columns:
                dfList[j] = dfList[j][columns_alt]

                dfList[j].rename(columns = {'idprovincia':'id_provincia',
                                            'iddepartamento': 'id_departamento',
                                            'categoria':'categoria',
                                            'cp':'codigo_postal',
                                            'telefono':'numero_de_telefono',
                                            'direccion':'domicilio'}, inplace = True)

def datosCleaner(df_list):
    for df in df_list:
        
        df.loc[df['mail'].isnull() , ['mail']] = 'NULL'
        df.loc[df['mail'] == np.nan , ['mail']] = 'NULL'
        df.loc[df['web'].isnull() , ['web']] = 'NULL'
        df.loc[df['web'] == 's/d' , ['web']] = 'NULL'
        df.loc[df['cod_loc'].isnull() , ['cod_loc']] = 'NULL'
        df.loc[df['id_provincia'].isnull() , ['id_provincia']] = 'NULL'
        df.loc[df['id_departamento'].isnull() , ['id_departamento']] = 'NULL'
        df.loc[df['categoria'].isnull() , ['categoria']] = 'NULL'
        df.loc[df['nombre'].isnull() , ['nombre']] = 'NULL'
        df.loc[df['localidad'].isnull() , ['localidad']] = 'NULL'
        df.loc[df['provincia'].isnull() , ['provincia']] = 'NULL'
        df.loc[df['domicilio'].isnull() , ['domicilio']] = 'NULL'
        df.loc[df['codigo_postal'].isnull() , ['codigo_postal']] = 'NULL'
        df.loc[df['numero_de_telefono'].isnull() , ['numero_de_telefono']] = 'NULL'
        df.loc[df['numero_de_telefono'] == 'nan' , ['numero_de_telefono']] = 'NULL'
        
        df = df.dropna(how='all')
        
def dateAdder(df_list):
    for df in df_list:
        df['fecha_de_carga'] = actual_date

def cinesCleaner(df):
    # Otro limpiador , pero preparado para el dataframe de cines
    # Descarta las columnas innecesarias, despues corrije la columna espacio_INCAA que tiene muchos espacios vacíos
    # Finalmente transforma el tipo a enteros y luego cuenta cada apricion de cada columna , según la provincia
    
    df = df[['Provincia','Butacas','Pantallas', 'espacio_INCAA']]
    
    df.loc[df['espacio_INCAA'] == 'SI' , ['espacio_INCAA']] = 1
    df.loc[df['espacio_INCAA'] == 'si' , ['espacio_INCAA']] = 1
    
    df = df.dropna()
    
    df['espacio_INCAA'] = df['espacio_INCAA'].astype(int)
    df['Butacas'] = df['Butacas'].astype(int)
    df['Pantallas'] = df['Pantallas'].astype(int)

    df = df.groupby(['Provincia']).sum()
    df = df.reset_index(level=0)
    df.columns = map(str.lower, df.columns)

    df['fecha_de_carga'] = actual_date
    return df

# Este pipeline, prepara los tres datasets limpiando las columnas no requeridas, basado en la primera consigna

def dataPipeline():
    
    DATASETS_ROOT_DIR = config('DATASETS_DIR')

    # Obtiene direcciones de mis archivos csv en base a mi rootdir
    fileDirs = directoryLister(DATASETS_ROOT_DIR)
    
    # Crea una lista con los dataframes en base a los csv
    df_list = dfListCreator(fileDirs)

    # Procesa cada dataframe para seleccionar ciertas columnas establecidas en la configuracion .env
    columns = ['cod_loc','idprovincia','iddepartamento','categoria','provincia','localidad','nombre','domicilio','cp','telefono','mail','web']
    columns_alt = ['cod_loc','idprovincia','iddepartamento','categoria','provincia','localidad','nombre','direccion','cp','telefono','mail','web']
    CHECKER = config('CHECKER')

    columnCleaner(df_list, columns, columns_alt)

    datosCleaner(df_list)
    dateAdder(df_list)

    
    return df_list

## Este pipeline prepara el datasets de cines, para ser subido a la base de datos, basado en la tercera consigna

def cinesPipeline():
    cinesDir = directoryLister('datasets/salas-de-cine')
    cinesDf = dfListCreator(cinesDir)
    cinesDf = cinesCleaner(cinesDf[-1]) # la última fecha
    
    
    return cinesDf

# Aquí creamos un ulitmo dataframe que intenta encarar la consigna dos, lamentablemente despues de pensarlo un buen rato no logro
# entender exactamente que estan pidiendo, por lo que propuse este dataframe que es interesante por que deja ver museos, cines
# y bibliotecas en cada provincia
def datosConjuntosPipeline(df_list):
    data_df = pd.concat(df_list)
    data_df =  data_df[['provincia','categoria','fecha_de_carga']]
    data_df = data_df.groupby(['provincia','categoria'],as_index=False).value_counts()
    data_df = data_df[['provincia','categoria','count','fecha_de_carga']]
    data_df
    return data_df

# Acá dejamos los dfs listos para ser subidos al servidor pasandolos por sus respectivos pipeline.


    

# Esta seccíon es la que se encarga de crear tablas y luego de subir todo al servidor

def serverPipeline():
    cines_df = cinesPipeline()
    df_list = dataPipeline()
    data_df = datosConjuntosPipeline(df_list)

    serverLoader.createTables()
    serverLoader.updateTablesCine(cines_df)

    serverLoader.updateTableDatos(df_list[0],'datos_bibliotecas')

    serverLoader.updateTableDatos(df_list[1],'datos_museos')

    # Borro las ultimas dos filas, por que hay problema al solicitar info desde la web del gobierno
    # Mi suposicion es que es un bug en el iterador del request.get, para chequear el problema deje un modo debug en el dataGatherer
    # Se puede activar cambiando el .env

    df_list[2]=df_list[2].drop(df_list[2].tail(2).index)
    serverLoader.updateTableDatos(df_list[2],'datos_cines')


    serverLoader.updateTablesDatosConjuntos(data_df)




