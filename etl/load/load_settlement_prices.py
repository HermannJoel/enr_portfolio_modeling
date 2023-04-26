import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

    
def load_settlement_prices_as_excel(dest_dir, src_flow, file_name, file_extension):
    scraped_date=(datetime.now()).strftime("%y_%m_%d")
    yesterday=(datetime.today() - timedelta(days=1)).strftime("%y_%m_%d")
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+'prices_mb.xlsx', 
                              index=False, sheet_name=f"mb_{prices_mb['Date'][0]}_scraped_{scraped_date}", 
                              float_format="%.3f")
        else: 
            src_flow.to_csv(dest_dir+'prices_mb.xlsx', index=False, 
                            sheet_name=f"mb_{prices_mb['Date'][0]}_scraped_{scraped_date}", 
                            encoding='utf-8-sig')
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))
        
        