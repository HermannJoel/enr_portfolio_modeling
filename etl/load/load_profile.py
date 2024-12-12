import pandas as pd
import os
import xlsxwriter
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
import logging.config

logger = logging.getLogger(__name__)

def load_profile(dest_dir, src_productible, src_profile_id, src_profile, src_mean_profile, file_name): 
    #if not src_productible.empty or not src_profile_id.empty or not src_profile.empty or not src_mean_profile.empty:
    logger.info('load profile starts!')
    try:
        #To export prod with no projet_id, profil with no projet_id, typical profil data as one excel file 
        #Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(dest_dir+file_name+'.xlsx', engine='xlsxwriter')
        #Write each dataframe to a different worksheet.
        src_productible.to_excel(writer, sheet_name="productible", float_format="%.4f", index=False)
        src_profile_id.to_excel(writer, sheet_name="profile_id", float_format="%.4f", index=False)
        src_profile.to_excel(writer, sheet_name="profile", float_format="%.4f", index=False) 
        src_mean_profile.to_excel(writer, sheet_name="mean_profile", float_format="%.4f", index=False)
        #Close the Pandas Excel writer and output the Excel file.
        writer.close()
        print(f"data loaded in {dest_dir} as {file_name}"+".xlsx!")
        #logger.info(f"{file_name}'+'.xlsx loaded to {dest_dir}")
        logger.info('load profile ends!')
    except Exception as e:
        print(f"data load as {file_name}"+".xlsx"+" "+"error!: "+str(e))
    # else:
    #     raise Exception(f"load {file_name}"+".xlsx failed! terminating ETL...")

def load_template_asset(dest_dir, src_flow, file_name, file_extension):
    logger.info('load template asset starts!')
    if file_name is not None:
        load_as_excel_file(dest_dir, src_flow, file_name, file_extension)
        #logger.info(f"{file_name}{file_extension} loaded to {dest_dir}")
        logger.info('load template asset ends!')
    else:
        raise Exception(f"load {file_name}{file_extension} failed! terminating ETL...")
    
