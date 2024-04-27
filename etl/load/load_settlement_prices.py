import pandas as pd
import os
import sys
from datetime import datetime, timedelta
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

    
def load_settlement_prices_as_excel(dest_dir, src_flow, file_name, file_extension):
    scraped_date=(datetime.now()).strftime("%y_%m_%d")
    yesterday=(datetime.today() - timedelta(days=1)).strftime("%y_%m_%d")
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(f"{dest_dir}"+f"{file_name}"+f"{file_extension}", 
                              index=False, sheet_name=f"mb_{src_flow['CotationDate'][0]}_scraped_{scraped_date}", 
                              float_format="%.3f")
        else: 
            src_flow.to_csv(f"{dest_dir}"+f"{file_name}"+f"{file_extension}", index=False, 
                            encoding='utf-8-sig', float_format="%.3f")
        print(f"Data loaded as {dest_dir}{file_name}{file_extension} successfully!")
    except Exception as e:
        print(f"Data load as {dest_dir}{file_name}{file_extension} error!: "+str(e))
        
        