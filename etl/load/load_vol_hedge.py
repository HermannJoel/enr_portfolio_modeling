import pandas as pd
import os
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def load_vol_hedge(dest_dir, src_flow, file_name, file_extension):
    load_as_excel_file(dest_dir, src_flow, file_name, file_extension)
    
    