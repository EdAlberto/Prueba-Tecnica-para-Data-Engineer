import pandas as pd
import os

# Devuelve los 2 archivos de entrada en formato Json 

def read_csv_files(ti=None):
    """
    Lee dos archivos CSV con Pandas, los convierte a formato JSON
    y los devuelve con return.
    """
    # Usar 'os.path.abspath' y 'os.getcwd()' es útil para depuración, 
    
    try:
        # 1. Leer archivo (journal_entries)
        df_transactions = pd.read_csv('/opt/airflow/dags/journal_entries.csv')
        
        # 2. Leer archivo (accounts)
        df_accounts = pd.read_csv('/opt/airflow/dags/accounts.csv')
        
        # Opcional: convertir 'transaction_date' a datetime
        df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])

        # Los DataFrames se serializan a JSON para ser devueltos en la tarea de inserción de datos.
        
        transactions_json = df_transactions.to_json(orient='records', date_format='iso')
        accounts_json = df_accounts.to_json(orient='records')

        print("Archivos CSV leídos y DataFrames serializados. Devolviendo resultado.")
        return {
            'journal_entries_df_json': transactions_json,
            'accounts_df_json': accounts_json
        }

    except FileNotFoundError as e:
        print(f"Error: Archivo no encontrado. Asegúrate de que '{e.filename}' existe y es accesible en la ruta: {os.getcwd()}")
        raise
    except Exception as e:
        print(f"Ocurrió un error durante la lectura de CSV: {e}")
        raise
