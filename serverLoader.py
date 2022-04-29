from decouple import config 
import psycopg2 

USER = config('USER')
HOST = config('HOST')
PORT = config('PORT')
PASSWORD = config('PASSWORD')
DBNAME = config('DBNAME')

def createTables():
    conn = psycopg2.connect(database = DBNAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

    cur = conn.cursor()

    with open('sqlScripts/creadorTablas.sql') as f:
        for line in f:
            cur.execute(line)

    #cur.execute("INSERT INTO cines(provincia, butacas, pantallas, espacio_incaa) VALUES ('Hola' , 2, 3,4);")
    #cur.execute("INSERT INTO cines(provincia, butacas, pantallas, espacio_incaa) VALUES ('Hola' , 2, 3,4);")

    conn.commit() 

    conn.close()
    cur.close()
    
    print('Tablas creadas')

def updateTablesCine(df):
    
    conn = psycopg2.connect(database = DBNAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
    cur = conn.cursor()


    for j in range(len(df)):
        provincia_column = df.iloc[j][0]
        butacas_column = df.iloc[j][1]
        pantallas_column = df.iloc[j][2]
        espacios_column = df.iloc[j][3]

        cur.execute("INSERT INTO cines(provincia,butacas,pantallas,espacio_incaa) \
                    VALUES ('{}',{},{},{});".format(provincia_column,butacas_column,pantallas_column,espacios_column))


    conn.commit() 

    conn.close()
    cur.close()

def updateTableDatos(df1,table_name):
    
    conn = psycopg2.connect(database = DBNAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
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
        
        # try:
        #     cur.execute("INSERT INTO {}(cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, \
        #                 nombre,domicilio, codigo_postal, num_telefono, mail ,web) VALUES ({},{},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}');"
        #                 .format(
        #                     table_name,cod_localidad,id_provincia,id_departamento,categoria,provincia,localidad,nombre,domicilio,
        #                     codigo_postal,numero,mail,web)
        #                 )
        # except:
        #     print(j)
        #     print("INSERT INTO {}(cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, \
        #                 nombre,domicilio, codigo_postal, num_telefono, mail ,web) VALUES ({},{},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}');"
        #                 .format(
        #                     table_name,cod_localidad,id_provincia,id_departamento,categoria,provincia,localidad,nombre,domicilio,
        #                     codigo_postal,numero,mail,web))


        
        
        cur.execute("INSERT INTO {}(cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, \
                        nombre,domicilio, codigo_postal, numero_de_telefono, mail ,web) VALUES ({},{},{},'{}','{}','{}','{}','{}','{}',{},'{}','{}');"
                        .format(
                            table_name,cod_localidad,id_provincia,id_departamento,categoria,provincia,str(localidad),nombre,domicilio,
                            codigo_postal,numero_de_telefono,mail,web)
                        )
        # except:
        #     print(table_name,cod_localidad,id_provincia,id_departamento,categoria,provincia,str(localidad),nombre,domicilio,
        #                     codigo_postal,numero,mail,web)
        
        # print("INSERT INTO {}(cod_localidad, id_provincia, id_departamento, categoria, provincia, localidad, \
        #                 nombre,domicilio, codigo_postal, num_telefono, mail ,web) VALUES (NULL,{},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}');"
        #                 .format(
        #                     table_name,cod_localidad,id_provincia,id_departamento,categoria,provincia,localidad,nombre,domicilio,
        #                     codigo_postal,numero,mail,web))

    # for j in range(len(df2)):
    #     provincia_column = df2.iloc[j][0]
    #     butacas_column = df2.iloc[j][1]
    #     pantallas_column = df2.iloc[j][2]
    #     espacios_column = df2.iloc[j][3]

    #     cur.execute("INSERT INTO cines(provincia,butacas,pantallas,espacio_incaa) \
    #                 VALUES ('{}',{},{},{});".format(provincia_column,butacas_column,pantallas_column,espacios_column))


    conn.commit() 

    conn.close()
    cur.close()

