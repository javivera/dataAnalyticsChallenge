Instrucciones para deploy.

Se debe crear un ambiente, yo utilizé miniconda, pero cualquiera puede ser utilizado.
La razón por la que no utilizé venv es por que noto que en la comunidad programadora se esta dejando de lado.

Para instalar miniconda usar instrucciones de la página
https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html

El programa fue escrito y probado con python 3.10.4.

Luego creamos y activamos un ambiente en conda
conda create --name <nombre del ambiente>
conda activate <nombre del ambiente>

Luego de crear el ambiente se requiere los siguiente paquetes que pueden ser instalados usando pip install <nombre paquete>

pandas
python-decouple
psycopg2
numpy
logging
sqlalchemy

Para configurar la conexión a la base de datos basta con acceder al archivo .env y completar los siguientes datos:

Usuario dentro del servidor (USERNAME)
Contraseña de dicho usuario (PASSWORD)
Host (HOST)
Puerto (PORT)
Nombre de la base de datos a la que se escribiran las tablas (DBNAME)

El .env debe estár en la misma carpeta que los scripts de python

Aclaración.
En un principio había entendido que Alkemy esperaba que NO usemos SQLAlchemy por eso hice un script (serverLoader) en donde se cargaba a mano (usando psycopg) todos los datos, sin usar df.to_sql. Finalmente al leer que se podía utilizar implementé serverLoaderAlkemy que obviamente es mucho mas simple y elegante. No obstante decidí dejar el antiguo serverLoader (funcionando) solo como muestra de trabajo.
Para utilizar serverLoader basta cambiar en el .env la opcion LOADERALT a True

Aclaración bis
Una mejor estructura para el programa hubiese separado serverLoaderAlkemy de dataRefiner.
Para esto el data refiner hubiera creado una nueva carpeta con los nuevos csv a partir de los dataframes creados y luego se usaría serverLoaderAlkemy para subir estos csv al servidor. Esto le daría mayor modulariadad al programa.
Al ser un programa pequeño y ser un proyecto de prueba considero es es redundante hacer esto.