import pandas as pd
from sqlalchemy import create_engine
import os

# db string format: postgres://username:password@localhost:5432/db_name
engine = create_engine("postgresql://postgres:postgres@localhost:5432/test")

pwd = os.getcwd()
print(pwd)
csv_dir = os.path.join(pwd,"raw_data") 
print(csv_dir)

for csv_file in os.listdir(csv_dir):
    
    if csv_file.endswith('.csv'):
        table_name = csv_file.replace('olist_', '').replace('_dataset.csv','')
        file_path = os.path.join(csv_dir, csv_file)
        print(file_path)
        print(f"Loading {table_name}...")
        df = pd.read_csv(file_path)

        df.to_sql(table_name, engine, if_exists='replace',index=False)
        print(f"Successfully loaded {len(df)} rows into {table_name}.")