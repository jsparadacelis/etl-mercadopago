# Pipeline MercadoPago

## Descripción

La solución se planteó como un sistema de ETL. La intención entonces es que los datos crudos se puedan comprimir en formato parquet haciendo que ocupen menos espacio y mejore la velocidad de lectura.

Para este escenario, se construyó un scrip en python que itera los archivos de un directorio llamado `ingestion` en el que estaría los datos crudos y los almacena comprimidos en otro directorio `compression`.

Una vez están los archivos comprimidos, se ejecuta un archivo `etl` que se encarga de leer los archivos parquet para despues crear tablas en una base de datos postgres que está montada en docker. La idea original era utilizar alguna base de datos especializada en temas de análisis como Snowflake o Redshift. Sin emabrgo noté ciertos problemas como por ejemplo que Snowflake no ofrece una imagen de docker que permita ejecutarlo en local.

Una vez los datos estan cargados dentro de la base de datos están disponibles en tablas. Ya con estas tablas se pueden hacer consultas, cruzar datos, etc. Esto permite que aquellos roles que tienen dominio de SQL puedan interactuar directament con los datos.

## Mejoras a futuro

La solución crea las tablas a partir de parquets utilizando la librería pandas y utilizando una conexión con SqlAlchemy. Si bien es cierto el proceso es simple y se puede reproducir para cualquier archivo, tiene la desventaja de no aplicar ciertas estrategias de optimización como indices o particiones. Agregar este tipo de soporte a la solución actual implica tener que evaluar cada tabla que se quiera agregar para saber sobre qué campos se puden aplicar dichas optimizaciones. Dentro del contexto de la prueba sería utilia añadir indices sobre el id del usuario y particiones sobre la fechas. 

Por otra parte, se utilizó un motor de base de datos que no estpa pensado a priori para soportar aplicaciones para análisis de grandes cantidades de datos. Existen otras bases de datos como las mencionadas anteriormente que están optimizadas para ese tipo de aplicaciones.

## ¿Cómo correr el programa?
Los scripts están escritos en python. Todo se orquesta desde el archivo `run.sh` desde la compresión de los archivos hasta la creación del dataset final.

La consulta con actual con la que se consturye el dataset final no añade los campos relacionados a la tabla pays. Tuve problemas con los tiempos de ejecución de la consulta así que decidí no añadir dichos campos. Sin embargo para añadir dichas columnas seguiría el mismo patron de las otras columnas. Generar tablas temporales para despues hacer los cruces por ID y fecha.
