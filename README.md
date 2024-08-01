# car_sales_ETL_python

Este script python se encarga de recopilar la informacion de la planilla excel Car_Sales-new.csv y realizar los procesos ETL para posteriormente crear un Datawearehouse en Postgres

Una vez descargado el codigo fuente, junto con la planilla y depositados en una carpeta es necesario:
- Crear la base de datos postgres con el nombre <b>cars_sales_datawarehouse</b>
- Configurar la linea 158 del script <b>load_data.py</b> con los parametros necesarios para la conexion al servidor de base de datos.
- Descargar las librerias necesarias de python para correrlas(ya sea localmente o en un entorno virtual).

 - Para crear el entorno virtual, abrimos la carpeta del proyecto con cmd y creamos el entorno:
     - python -m venv venv

 - Luego activamos el entorno virtual:
     - .\venv\Scripts\activate  

- Una vez activado instalamos las librerias con el archivo requirements.txt:
     - pip install -r requirements.txt

- Cuando todo se encuentre instalado lo corremos con el comando:
    - py load_data.py


<b>El script crea todas la estructura de tablas y posteriormente realiza los procesos ETL para su ingesta en Postgres.</b>
