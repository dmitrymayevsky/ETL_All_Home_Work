from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def full_load():
    source = '/opt/airflow/output/temperature_analysis.csv'
    if not os.path.exists(source):
        raise FileNotFoundError(f"File {source} not found")
    
    df = pd.read_csv(source)
    print(f"Full load: {len(df)} records")
    
    target_dir = '/opt/airflow/output/full_load'
    os.makedirs(target_dir, exist_ok=True)
    
    target = f'{target_dir}/full_{datetime.now().strftime("%Y%m%d")}.csv'
    df.to_csv(target, index=False)
    
    print(f"Saved to: {target}")
    return len(df)

with DAG(
    dag_id='full_temperature_load',
    start_date=datetime(2024, 1, 1),
    schedule='@monthly',
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='full_load_task',
        python_callable=full_load
    )
