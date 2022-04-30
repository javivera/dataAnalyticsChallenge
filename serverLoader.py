from sqlalchemy import table
from decouple import config 
from decouple import AutoConfig
import os

config = AutoConfig(search_path=os.getcwd())
import psycopg2 

USERNAME = config('USERNAME')
HOST = config('HOST')
PORT = config('PORT')
PASSWORD = config('PASSWORD')
DBNAME = config('DBNAME')

def createTables():
    try:
        conn = psycopg2.connect(database = DBNAME, user = USERNAME, password = PASSWORD, host = HOST, port = PORT)
        cur = conn.cursor()

        with open('sqlScripts/creadorTablas.sql') as f:
            for line in f:
                cur.execute(line)

        conn.commit() 
        conn.close()
        cur.close()
        
        print('Tablas creadas exitosamente')

    except:
        print('Hubo un problema al crear las tablas')

def updateTablesCine(df):
    try:
        conn = psycopg2.connect(database = DBNAME, user = USERNAME, password = PASSWORD, host = HOST, port = PORT)
        cur = conn.cursor()

        for j in range(len(df)):
            provincia_column = df.iloc[j][0]
            butacas_column = df.iloc[j][1]
            pantallas_column = df.iloc[j][2]
            espacios_column = df.iloc[j][3]
            fecha_carga_column = df.iloc[j][4]

            cur.execute("INSERT INTO cines(provincia,butacas,pantallas,espacio_incaa,fecha_de_carga) \
                        VALUES ('{}',{},{},{},'{}');".format(provincia_column,butacas_column,
                        pantallas_column,espacios_column,fecha_carga_column))


        conn.commit() 
        
        conn.close()
        cur.close()
        print('Cines fue subido con éxito')
    except:
        print('Hubo algún problema con la tabla Cines')
        pass

def updateTableDatos(df1,table_name):
    
    try:
        conn = psycopg2.connect(database = DBNAME, user = USERNAME, password = PASSWORD, host = HOST, port = PORT)
        cur = conn.cursor()

        for j in range(len(df1)):
            cod_localidad = df1.iloc[j][0]
            id_provincia = df1.iloc[j][1]
            id_departamento = df1.iloc[j][2]
            categoria = df1.iloc[j][3]
            provincia = df1.iloc[j][4]
            localidad = df1.iloc[j][5]
            nombre = df1.iloc[j][6]
            domicilio = df1.iloc[j][7]
            codigo_postal = df1.iloc[j][8]
            numero_de_telefono = df1.iloc[j][9]
            mail = df1.iloc[j][10]
            web = df1.iloc[j][11]
            fecha_carga_column = df1.iloc[j][12]

            cur.execute("INSERT INTO {}(cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, \
                                nombre,domicilio, codigo_postal, numero_de_telefono, mail ,web,fecha_de_carga) VALUES ({},{},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');"
                                .format(
                                    table_name,cod_localidad,id_provincia,id_departamento,categoria,provincia,str(localidad),nombre,domicilio,
                                    codigo_postal,numero_de_telefono,mail,web,fecha_carga_column)
                                )
                                
        conn.commit() 
        conn.close()
        cur.close()
        print('{} fue subido con éxito'.format(table_name))

    except:
        print('Hubo algún problema con {}'.format(table_name))
        pass

def updateTablesDatosConjuntos(df):
    try:
        conn = psycopg2.connect(database = DBNAME, user = USERNAME, password = PASSWORD, host = HOST, port = PORT)
        cur = conn.cursor()


        for j in range(len(df)):
            provincia_column = df.iloc[j][0]
            categorias_column = df.iloc[j][1]
            cantidad_column = df.iloc[j][2]
            fecha_carga_column = df.iloc[j][3]
            

            cur.execute("INSERT INTO datos_conjuntos (provincia,categorias,cantidad,fecha_de_carga) \
                        VALUES ('{}','{}',{},'{}');".format(provincia_column,categorias_column,cantidad_column,fecha_carga_column))


        conn.commit() 

        conn.close()
        cur.close()
        print('Datos Conjuntos fue subido con éxito')

    except:
        print('Hubo algún problema con datos conjuntos')
        pass