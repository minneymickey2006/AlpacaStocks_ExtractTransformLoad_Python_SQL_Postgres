import pandas as pd 
from sqlalchemy import Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.dialects import postgresql
import os 

class Load():

    @staticmethod
    def load(
            df: pd.DataFrame, 
            load_target:str, 
            load_method:str="upsert",
            target_file_directory:str=None,
            target_file_name:str=None,
            target_database_engine=None,
            target_table_name:str=None
        )->None:
        """
        Load dataframe to either a file or a database. 
        - df: pandas dataframe to load.  
        - load_target: choose either `file` or `database`.
        - load_method: currently only `upsert` is supported. 
        - target_file_directory: directory where the file will be written to in parquet format.
        - target_file_name: name of the target file e.g. stock.parquet. 
        - target_database_engine: SQLAlchemy engine for the target database. 
        - target_table_name: name of the SQL table to create and/or upsert data to. 
        """
        
        if load_target == "file": 
            if load_method == "upsert": 
                # upsert (update and insert) data to a csv file 
                if target_file_name in os.listdir(f"{target_file_directory}/"): 
                    df_current = pd.read_parquet(f"{target_file_directory}/{target_file_name}")
                    df_concat = pd.concat(objs=[df_current,df[~df.index.isin(df_current.index)]]) # ~: converts true to false, and false to true. 
                    df_concat.to_parquet(f"{target_file_directory}/{target_file_name}")
                else:
                    # create file 
                    df.to_parquet(f"{target_file_directory}/{target_file_name}")
        elif load_target == "database": 
            # create target table if not exists 
            meta = MetaData()
            stock_price_tesla_table = Table(
                target_table_name, meta, 
                Column("timestamp", String, primary_key=True),
                Column("exchange", String, primary_key=True),
                Column("price", Float),
                Column("size", Integer)
            )
            meta.create_all(target_database_engine) # creates table if it does not exist 
            insert_statement = postgresql.insert(stock_price_tesla_table).values(df.to_dict(orient='records'))
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['timestamp', 'exchange'],
                set_={c.key: c for c in insert_statement.excluded if c.key not in ['timestamp','exchange']})
            target_database_engine.execute(upsert_statement)

        else:
            raise Exception("The parameters passed in results in no action being performed.")