from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

def process_temperature_data():
    file_path = '/opt/airflow/dags/IOT-temp.csv'
    df = pd.read_csv(file_path)
    
    print(f"Original data shape: {df.shape}")
    print(df.head())
    
    df = df[df['out/in'] == 'In']
    print(f"After filtering 'In': {df.shape}")
    
    #HotFix, увидел проблемы с форматами дат при выполнении рана
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    print("Date converted with day-first format")
    
    # Очистка по перцентилям
    temp_lower = df['temp'].quantile(0.05)
    temp_upper = df['temp'].quantile(0.95)
    df_clean = df[(df['temp'] >= temp_lower) & (df['temp'] <= temp_upper)]
    print(f"After percentile cleaning (5-95%): {df_clean.shape}")
    print(f"Percentiles - 5th: {temp_lower:.2f}, 95th: {temp_upper:.2f}")
    
    df_clean['year'] = df_clean['noted_date'].dt.year
    
    results = []
    for year in sorted(df_clean['year'].unique()):
        year_data = df_clean[df_clean['year'] == year]
        
        hottest = year_data.nlargest(5, 'temp')[['noted_date', 'temp']]
        coldest = year_data.nsmallest(5, 'temp')[['noted_date', 'temp']]
        
        for _, row in hottest.iterrows():
            results.append({
                'year': year,
                'date': row['noted_date'].strftime('%Y-%m-%d'),
                'temperature': row['temp'],
                'type': 'hottest'
            })
        
        for _, row in coldest.iterrows():
            results.append({
                'year': year,
                'date': row['noted_date'].strftime('%Y-%m-%d'),
                'temperature': row['temp'],
                'type': 'coldest'
            })
    
    result_df = pd.DataFrame(results)
    
    output_path = '/opt/airflow/output/temperature_analysis.csv'
    os.makedirs('/opt/airflow/output', exist_ok=True)
    result_df.to_csv(output_path, index=False)
    
    print("\n=== Results ===")
    print(f"Total records: {len(result_df)}")
    print("\nSample of hottest days:")
    print(result_df[result_df['type'] == 'hottest'].head(10))
    print("\nSample of coldest days:")
    print(result_df[result_df['type'] == 'coldest'].head(10))
    
    summary = result_df.groupby(['year', 'type']).agg({
        'temperature': ['min', 'max', 'mean']
    }).round(2)
    print("\n=== Summary by year ===")
    print(summary)
    
    return output_path

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='iot_temperature_analysis',
    default_args=default_args,
    catchup=False,
    tags=['iot', 'temperature', 'analysis']
) as dag:
    
    process_task = PythonOperator(
        task_id='process_temperature_data',
        python_callable=process_temperature_data
    )
