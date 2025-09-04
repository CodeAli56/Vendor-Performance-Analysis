import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import time


ingestion_logger = logging.getLogger("ingestion")
ingestion_handler = logging.FileHandler("logs/ingestiondb.log", mode="a")
ingestion_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
ingestion_logger.addHandler(ingestion_handler)
ingestion_logger.setLevel(logging.DEBUG)


engine = create_engine("sqlite:///inventory.db")

def ingest_db(df, table_name, engine):
    '''This function loads the dataframe into actual database.'''

    df.to_sql(table_name, con = engine, if_exists="replace", index = False)

def load_raw_data():
    '''This function converts the csv files into dataframe and pass it to ingest_db function'''
    
    start = time.time()
    for file in os.listdir("data"):
        if ".csv" in file:
            df = pd.read_csv("data/"+file)
            ingestion_logger.info(f"Ingesting {file} in db")
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    ingestion_logger.info("----------Ingestion Complete----------")
    ingestion_logger.info(f"Total time taken: {total_time} minutes")


if __name__ == "__main__":
    load_raw_data()