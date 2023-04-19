from contractsprices_planif import*
from contractsprices_ppa import*
from contractsprices_prod import*
import pandas as pd
import configparser
import os
pd.options.mode.chained_assignment=None
from functions import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini')
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])
prices=os.path.join(os.path.dirname("__file__"),config['develop']['template_prices'])


if __name__ == '__main__':
    
    #contracts_prices_ppa
    df_template_asset, df_ppa=Extract(template_asset_path=template_asset, ppa_path=ppa)
    prices_ppa=transform(template_asset=df_template_asset, df_ppa=df_ppa)
    #contracts_prices_planif
    etl_contract_prices=etl_contract_prices(template_hedge)
    etl_contract_prices.transform_prices_planif(etl_contract_prices.df_hedge_ = etl_contract_prices.extract_data())
    #contracts_prices_prod
    
    def load_template_prices(dest_dir, src_flow, file_name, file_extension):
    """Function to load data as excel file     
    parameters
    ==========
    dest_dir (str) :
        target folder path
    src_flow (DataFrame) :
        data frame returned by transform function        
    file_name (str) : 
        destination file name
    exemple
    =======
    Load(dest_dir, template_asset_without_prod, 'template_asset', '.csv')
    >>> to load template_asset_without_prod in dest_dir as template_asset.csv 
    """
    try:
        if file_extension in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            src_flow.to_excel(dest_dir+file_name+file_extension, index=False, float_format="%.4f")
        else: 
            src_flow.to_csv(dest_dir+file_name+file_extension, index=False, float_format="%.4f", encoding='utf-8-sig')
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))
        
