import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
import logging.config

logger = logging.getLogger(__name__)

def load_contract_prices_all(dest_dir, src_flow, file_name, file_extension):
    if file_name is not None:
        load_as_excel_file(dest_dir, src_flow, file_name, file_extension)
        logger.info(f"{file_name}{file_extension} loaded to {dest_dir}")
    else:
        raise Exception(f"load {file_name}{file_extension} failed! terminating ETL...")
