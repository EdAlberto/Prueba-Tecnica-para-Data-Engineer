from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pandas as pd
from typing import Dict, Any
from data_migration_flow import read_csv_files 


# ID de la conexión de PostgreSQL configurada en Airflow UI
POSTGRES_CONN_ID = 'postgres_localhost' 
TRANSACTIONS_TABLE = 'journal_entries'
ACCOUNTS_TABLE = 'accounts'
SUMMARY_TABLE = 'financial_summary'

#Creación del DAG
@dag(
    dag_id='etl_csv_to_postgres_refactored',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['pandas', 'postgres', 'return']
)
def csv_to_postgres_pipeline_return():
    
    #Tarea 2: Creación de tablas para ambos archivos en SQL (PosgreSQL)
    create_transactions_table = SQLExecuteQueryOperator(
        task_id=f'create_table_{TRANSACTIONS_TABLE}',
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TRANSACTIONS_TABLE} (
                transaction_id BIGINT,
                transaction_date DATE,
                account_number VARCHAR(20),
                amount NUMERIC(10, 2)
            );
        """
    )

    create_accounts_table = SQLExecuteQueryOperator(
        task_id=f'create_table_{ACCOUNTS_TABLE}',
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {ACCOUNTS_TABLE} (
                account_number VARCHAR(20),
                account_name VARCHAR(100)
            );
        """
    )
    
    #Tarea 3: Leer CSVs
    @task(task_id='read_and_return_dfs')
    def read_files_task():
        # Llama a la función. El diccionario
        # bajo return.
        return read_csv_files()
        
    data_output = read_files_task() # La variable contiene los dos objetos json devueltos

    #Tarea 4: Cargar Datos en PostgreSQL
    @task(task_id='load_data_to_postgres_pandas')
    def load_data_to_postgres(data_dict: Dict[str, Any]):
        
        # 1. Airflow pasa el diccionario completo como argumento 'data_dict'
        transactions_json = data_dict['journal_entries_df_json']
        accounts_json = data_dict['accounts_df_json']

        # 2. Convertir JSON strings de vuelta a DataFrames
        df_transactions = pd.read_json(transactions_json, orient='records')
        df_accounts = pd.read_json(accounts_json, orient='records')

        # 3. Establecer conexión y cargar
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()
        
        print("Conexión a PostgreSQL establecida. Iniciando carga de datos.")

        # Cargar journal_entries
        df_transactions.to_sql(name=TRANSACTIONS_TABLE, con=engine, if_exists='append', index=False)
        print(f"Datos de {TRANSACTIONS_TABLE} cargados exitosamente.")

        # Cargar Cuentas
        df_accounts.to_sql(name=ACCOUNTS_TABLE, con=engine, if_exists='append', index=False)
        print(f"Datos de {ACCOUNTS_TABLE} cargados exitosamente.")

    # Al llamar a la tarea, le pasamos el objeto de la tarea anterior (data_output)
    load_data = load_data_to_postgres(data_dict=data_output)

    #Tarea para crear la tabla de transformación de datos parte 1
    # Tarea 4a: Crear la tabla resumen de validación de saldos
    create_summary_table = SQLExecuteQueryOperator(
        task_id=f'create_table_{SUMMARY_TABLE}',
        conn_id=POSTGRES_CONN_ID,
        # Definición de la nueva tabla
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
                transaction_id BIGINT,
                transaction_date DATE,
                account_number VARCHAR(20),
                account_name VARCHAR(100),
                debit_amount NUMERIC(10, 2),
                credit_amount NUMERIC(10, 2),
                is_valid_transaction BOOLEAN
            );
        """
    )
    
    # Tarea 4b: Generar la tabla resumen con transformaciones
    transform_and_load_summary = SQLExecuteQueryOperator(
        task_id='generate_summary_table',
        conn_id=POSTGRES_CONN_ID,
        # TRUNCATE es importante para evitar duplicados si el DAG se re-ejecuta
        sql=f"""
            TRUNCATE TABLE {SUMMARY_TABLE}; 
            
            INSERT INTO {SUMMARY_TABLE} (
                transaction_id, 
                transaction_date, 
                account_number, 
                account_name, 
                debit_amount, 
                credit_amount, 
                is_valid_transaction
            )
            SELECT
                t.transaction_id,
                t.transaction_date,
                t.account_number,
                -- Nombre de la cuenta
                COALESCE(a.account_name, 'Unknown Account') AS account_name,
                
                -- Monto de Débito (si el monto es positivo)
                CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END AS debit_amount,
                
                -- Monto de Crédito (si el monto es negativo, lo hacemos positivo para la columna)
                CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END AS credit_amount,
                
                -- Validación (Fecha en 2024 Y cuenta existe)
                (EXTRACT(YEAR FROM t.transaction_date) = 2024 AND a.account_name IS NOT NULL) AS is_valid_transaction
            FROM {TRANSACTIONS_TABLE} t
            LEFT JOIN {ACCOUNTS_TABLE} a
                ON t.account_number = a.account_number;
        """
        
    )

    # Tarea 5: Crear el reporte de validación de saldos
    @task(task_id='validate_balance_integrity')
    def check_balance_integrity():
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        validation_query = f"""
            WITH BalanceCheck AS (
                SELECT
                    t.transaction_id,
                    -- Verificar si la suma de Débito y Crédito coincide con el valor original (absoluto)
                    ABS(t.amount) AS original_abs_amount,
                    s.debit_amount + s.credit_amount AS summary_total_amount
                FROM {TRANSACTIONS_TABLE} t
                JOIN {SUMMARY_TABLE} s ON t.transaction_id = s.transaction_id
            )
            SELECT
                transaction_id,
                original_abs_amount,
                summary_total_amount,
                'INVALID' AS status
            FROM BalanceCheck
            -- Si la suma del resumen no coincide con el monto original (absoluto)
            WHERE original_abs_amount != summary_total_amount
            LIMIT 10;
        """

        integrity_errors = postgres_hook.get_pandas_df(sql=validation_query)

        #Imprimir el resultado en los logs de Airflow
        print("--- INICIO DE VALIDACIÓN DE INTEGRIDAD DE SALDOS ---")
        if integrity_errors.empty:
            print("✅ Validación de Saldos exitosa: ¡No se encontraron transacciones inválidas para la migración!")
        else:
            print(f"❌ ¡ATENCIÓN! Se encontraron {len(integrity_errors)} transacciones con saldos no cuadrados (Débito != Crédito). Estas son inválidas.")
            # Imprimir el DataFrame en formato legible para el log
            print(integrity_errors.to_markdown(index=False)) 
            # Generar el reporte en formato .csv 
            integrity_errors.to_csv('/opt/airflow/dags/balance_check.csv')

        print("--- FIN DE VALIDACIÓN DE VALIDACIÓN DE SALDOS ---")
    
    validate_integrity = check_balance_integrity()

    # Tarea 6: Crear el reporte de resumen de cuentas
    @task(task_id='summarize_final_balances')
    def summarize_balances():
        
        # 1. Establecer conexión con PostgreSQL usando Hook
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 2. Definir la consulta de resumen de saldos
        summary_query = f"""
            SELECT
                account_name,
                -- Saldo Final = SUM(Débito) - SUM(Crédito)
                SUM(debit_amount) - SUM(credit_amount) AS final_balance
            FROM {SUMMARY_TABLE}
            -- SOLO transacciones que pasaron la validación de calidad
            WHERE is_valid_transaction = TRUE
            GROUP BY account_name
            ORDER BY final_balance DESC;
        """
        
        # 3. Ejecutar la consulta y obtener el DataFrame de resultados
        final_balances_df = postgres_hook.get_pandas_df(sql=summary_query)

        # 4. Imprimir el resultado en los logs de Airflow
        print("--- INICIO DE RESUMEN DE CUENTAS ---")
        if final_balances_df.empty:
            print("⚠️ No se encontraron saldos para transacciones válidas.")
        else:
            print("✅ Saldo final de cuentas (válidas), ordenado de mayor a menor:")
            # Imprimir el DataFrame en formato legible para el log
            print(final_balances_df.to_markdown(index=False))
            # Imprimir el reporte
            final_balances_df.to_csv('/opt/airflow/dags/accounts_summary.csv')
        print("--- FIN DE RESUMEN DE RESUMEN DE CUENTAS ---")

    summarize_data = summarize_balances()

    # Definir la secuencia de tareas
    [create_transactions_table, create_accounts_table] >> data_output >> load_data >> create_summary_table >> transform_and_load_summary >> validate_integrity >> summarize_data

# Instanciar el DAG
csv_to_postgres_pipeline_return()