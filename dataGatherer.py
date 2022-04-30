import re
import requests
import json
import logging
import csv
import os

from datetime import datetime

# Configuración necesaria para que decouple detecte .env (donde están guardadas las configuraciones)
from decouple import config
from decouple import AutoConfig

config = AutoConfig(search_path=os.getcwd())

# Retrieves useful date information

now = datetime.now()

current_date = now.strftime("%d-%B-%Y")
current_month_int = now.strftime("%m")
current_month = now.strftime("%B")
current_year = current_date[-4:]
current_date = now.strftime("%d-%m-%Y")

# # Si lo tuvieramos que hacer en español usariamos este diccionario

# calendar = {'01':'Enero',
#             '02':'Febrero',
#             '03':'Marzo',
#             '04':'Abril',
#             '05':'Mayo',
#             '06':'Junio',
#             '07':'Julio',
#             '08':'Agosto',
#             '09':'Septiembre',
#             '10':'Octubre',
#             '11':'Noviembre',
#             '12':'Diciembre',
# }

# now = datetime.now()

# current_time = now.strftime("%d-%m-%Y")
# current_month_int = now.strftime("%m")

# current_month = calendar[current_month_int]
# current_year = current_time[-4:]

DATASETS_ROOT_DIR = config('DATASETS_ROOT_DIR')


def createFolderStructure(datasetName):
    # Create la estructura de archivos necesaria
    root = DATASETS_ROOT_DIR + "/" + datasetName
    directory = current_year + "-" + current_month

    path = os.path.join(root, directory)
    if not os.path.exists(path):
        os.makedirs(path)


def retrieve_and_save(datasetSource, datasetName, DEBUG=False):
    # Pide los datasets desde las webs y los parsea en un csv
    # Aclaración el regex es por que hay ciertos datos que estan mal formateados ejemplo "Juan , Pablo" ,
    # esto debería estar en la misma columna. Pero en un CSV una coma (o el separador que se utilize)
    # significa una nueva columna. Utilizando el regex identificamos estos casos molestos,
    # la desventaja es que es mas lento la ventaja es que ganamos 141+4+27 filas mas
    createFolderStructure(datasetName)

    r = requests.get(datasetSource)
    if r.status_code != 200:
        print("Failure!!")
        exit()

    path = os.path.join(DATASETS_ROOT_DIR, datasetName,
                        current_year + "-" + current_month)

    with open(path + '/' + datasetName + '-' + current_date + '.csv', 'w') as f:

        writer = csv.writer(f)
        contador = 0

        for line in r.iter_lines():
            decoded_line = line.decode('utf-8')

            if contador > 329 and DEBUG:
                logging.debug('Linea número ' + str(contador) + decoded_line)

            if '"' in decoded_line:
                pattern = r'\".*,.*\"'
                x = re.findall(pattern, line.decode('utf-8'))

                if len(x) != 0:
                    replacement = x[0].replace(',', ' ')
                    decoded_line = decoded_line.replace(x[0], replacement)

            if "'" in decoded_line:
                decoded_line = decoded_line.replace("'", "''")

            if '’' in decoded_line:
                decoded_line = decoded_line.replace("’", "''")
            if '`' in decoded_line:
                decoded_line = decoded_line.replace("`", "''")

            if '"' in decoded_line:
                decoded_line = decoded_line.replace('"', "''")

            if 's/d' in decoded_line:
                decoded_line = decoded_line.replace('s/d', 'NULL')

            if ',,' in decoded_line:
                # print(decoded_line)
                # dejo este print comentado por que es una herramienta útil que muestra el proceso lógico que utilizé
                # para correjir los errores que tenian los datas pedidos a las páginas
                decoded_line = decoded_line.replace(',,', ',NULL,')

            contador += 1
            writer.writerow(decoded_line.split(','))


def dataGatherer():
    web1 = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv"
    web2 = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv"
    web3 = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"

    DEBUG = config('DEBUG', cast=bool)
    try:
        retrieve_and_save(web1, 'museos')
        retrieve_and_save(web2, 'salas-de-cine', DEBUG)
        retrieve_and_save(web3, 'bibliotecas-populares')
        logging.debug("Los datos fueron recolectados con éxito")

    except:
        logging.debug("Hubo un problema en la recolección de datos")
