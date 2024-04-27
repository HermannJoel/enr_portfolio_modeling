# -*- coding: utf-8 -*-
"""
Created on Fri Jul  8 11:54:42 2022

@author: hermann.ngayap
"""
from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objs as go
from src.utils.functions import make_dbc_table
from dashboards.env.colors import colors
from dashboards.env.x_axes import*
from queries.pg_dwh_queries import prod_m_y, prod_m_hcr_y, prod_y, hcr_y

cols = ['Year', 'Prod-Merchant', 'HCR-Pm', 'Prod-Total', 'HCR-T']

frames=[prod_m_y, prod_m_hcr_y.iloc[:,1:], prod_y.iloc[:,1], hcr_y.iloc[:,1]]
df=pd.concat(frames, axis=1, ignore_index=False)

table_header = [
     html.Thead(html.Tr([html.Th(i) for i in cols]))] 

table_body = [
         html.Tbody(
             [
                 html.Tr([html.Td(df.iloc[i][col]) for col in df.columns]) 
                 for i in range(len(df))
                 ]
             )
         ]

prod_merchant_tbl=html.Div(
    children=[
     dbc.Table(
         table_header + table_body,
         bordered=False,
         responsive=True,
         hover=True,
         striped=True,
         #style=table,
         className="table")
     ]
 )



