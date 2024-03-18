# Pipeline MercadoPago

## Descripción

La solución se planteó como un sistema de ETL. La intención entonces es que los datos crudos se puedan comprimir en formato parquet haciendo que ocupen menos espacio y mejore la velocidad de lectura.

Para este escenario, se construyó un scrip en python que itera los archivos de un directorio llamado `ingestion` en el que estaría los datos crudos y los almacena comprimidos en otro directorio `compression`.

Una vez están los archivos comprimidos, se ejecuta un archivo `etl` que se encarga de leer los archivos parquet para despues crear tablas en una base de datos postgres. La idea original era utilizar alguna base de datos especializada en temas de análisis como Snowflake o Redshift. Sin emabrgo noté ciertos problemas como por ejemplo que Snowflake no ofrece una imagen de docker que permita ejecutarlo en local.

Una vez los datos estan cargados dentro de la base de datos están disponibles en tablas. Ya con estas tablas se pueden hacer consultas, cruzar datos, etc. Esto permite que aquellos roles que tienen dominio de SQL puedan interactuar directament con los datos.



