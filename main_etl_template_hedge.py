from data_pipelines.etl_template_hedge import*
import os
import configparser
pd.options.mode.chained_assignment=None

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)

# Initialize Variables
vmr=os.path.join(os.path.dirname("__file__"),config['develop']['vmr'])
planif=os.path.join(os.path.dirname("__file__"),config['develop']['planif'])
hedge_vmr=os.path.join(os.path.dirname("__file__"),config['develop']['hedge_vmr'])
hedge_planif=os.path.join(os.path.dirname("__file__"),config['develop']['hedge_planif'])


if __name__ == '__main__':
    df_hedge_vmr, df_hedge_planif=Extract(hedge_vmr_path=hedge_vmr, hedge_planif_path=hedge_planif)
    template_hedge=transform(hedge_vmr=df_hedge_vmr, hedge_planif=df_hedge_planif)
    Load(dest_dir=dest_dir, src_flow=template_hedge, file_name="template_hedge", file_extension='.csv')