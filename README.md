Instrucciones para deploy.

Se debe crear un ambiente, yo utilizé miniconda, pero cualquiera puede ser utilizado.

El programa fue escrito y probado con python 3.10.4.

Luego de crear el ambiente se requiere los siguiente paquetes que pueden ser instalados usando pip install <nombre paquete>

pandas
python-decouple
psycopg2
numpy

Para configurar la conexión a la base de datos basta con acceder al archivo .env y completar los siguientes datos:

Usuario dentro del servidor (USERNAME)
Contraseña de dicho usuario (PASSWORD)
Host (HOST)
Puerto (PORT)
Nombre de la base de datos a la que se escribiran las tablas (DBNAME)
