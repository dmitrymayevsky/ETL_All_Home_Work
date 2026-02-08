from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

def incremental_load():
    days_back = 3
    source = '/opt/airflow/output/temperature_analysis.csv'
    
    if not os.path.exists(source):
        raise FileNotFoundError(f"File {source} not found")
    
    df = pd.read_csv(source)
    df['date'] = pd.to_datetime(df['date'])
    
    cutoff = datetime.now() - timedelta(days=days_back)
    new_data = df[df['date'] >= cutoff].copy()
    
    print(f"Incremental load: {len(new_data)} new records")
    
    if len(new_data) == 0:
        print("No new data")
        return 0
    
    target_dir = '/opt/airflow/output/incremental'
    os.makedirs(target_dir, exist_ok=True)
    
    target = f'{target_dir}/inc_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    new_data.to_csv(target, index=False)
    
    print(f"Saved to: {target}")
    return len(new_data)

with DAG(
    dag_id='incremental_temperature_load',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='incremental_load_task',
        python_callable=incremental_load
    )
