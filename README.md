# Prueba-Tecnica-para-Data-Engineer

Pasos para ejecutar el código (Windows).

1. tener Airflow instalado en un contenedor de Docker
1.0 Crear una carpeta de Airflow, en vscode ahora seguir los pasos.
1.1 Con base al archivo docker-compose.yaml que se descarga de la página oficial de Airflow (3.0.5) 
1.2 Se agrega el puerto de Postgres como base de datos en el archivo:
    ports:
      - 5432:5432
1.4 se crea el archivo .env con el contenido:
	AIRFLOW_IMAGE_NAME=apache/airflow:3.0.0
	AIRFLOW_UID=50000
1.5 Se crea el contenedor con Airflow.

1.6 Se enciende el contenedor con Airflow y se valida también la conexión a la base de datos de Postgres

1.7 se prueba Airflow en el navegador: http://localhost:8080/

2. En vs code, se abre una nueva sesión, apuntando al contenedor donde está airflow. (descargar el paquete: Dev Containers)

2.1 En la parte inferior izquierda está el símbolo: >< para abrir una ventana remota apuntando al contenedor y seleccionar: Attach to running container.

2.2 Ya en la sesión dentro del contenedor se dirige a: /opt/airflow/dags

2.3 En esta carpeta se crean los archivos .py (dag y lectura de los dos archivos .csv), también se agregan los dos archivos .csv que son las fuentes de datos.

2.4 Los archivos deben tener esta ruta para su lectura: /opt/airflow/dags/journal_entries.csv, de lo contrario el DAG no los detecta.

3. Ejecución del DAG

3.1 Entrar a Airflow http://localhost:8080/ (contraseña por defecto en el archivo docker-compose.yaml)

3.2 Ir a la sección de Admin y crear una nueva conexión de postgres (postgres_localhost)

3.3 En host, debe ir: host.docker.internal, en login airflow y la contraseña predeterminada en docker-compose.yaml, la base de datos para esta prueba es: postgres.

3.4 Se guarda la conexión.

3.5 Ir a Dags, buscar el nombre del dag creado (etl_csv_to_postgres_refactored) y hacer click en Trigger. Así se ejecuta el dag donde se puede ver en cada tarea o task los outputs como logs.

3.6 los reportes generados quedan en la carpeta dags.

######### Archivos ######

- El archivo del dag es: dag_with_postgres_operator (modificar los nombres generan un error y para este ejercicio el nombre del dag y el archivo .py tienen los nombres descritos), contiene toda la lógica de creación de
la tabla de resumen financiero (transformación de registros) y las consultas (validacion de calidad parte 2)

- El archivo con la lógica de lectura de los archivos .csv es data_migration_flow.py
