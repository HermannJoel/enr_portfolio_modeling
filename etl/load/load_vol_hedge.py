import pandas as pd
import os
os.chdir('D:/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def load_vol_hedge(dest_dir, src_flow, file_name, file_extension):
    load_as_excel_file(dest_dir, src_flow, file_name, file_extension)
    
    