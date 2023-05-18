from database.postgres import PostgresDB
from alpaca_stocks.etl.extract import Extract
from alpaca_stocks.etl.transform import Transform
from alpaca_stocks.etl.load import Load
from utility.date_time import DateTime
import os
import logging
import yaml
from io import StringIO
from utility.metadata_logging import MetadataLogging
import datetime as dt
from apscheduler.schedulers.background import BackgroundScheduler
import time

def pipeline_per_stock(config, run_log)->bool:
    
    # start the streamIO from a clean slate
    run_log.seek(0)
    run_log.truncate(0)
    
    api_key_id = os.environ.get("api_key_id")
    api_secret_key = os.environ.get("api_secret_key")
    
    metadata_logger = MetadataLogging()
    
    