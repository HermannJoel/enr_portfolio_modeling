import pandas as pd
import os
import xlsxwriter
import sys
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*

def load_profile(dest_dir, src_productible, src_profile_id, src_profile, src_mean_profile, file_name):
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
        writer.save()
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))

def load_template_asset(dest_dir, src_flow, file_name, file_extension):
    load_as_excel_file(dest_dir, src_flow, file_name, file_extension)
    
