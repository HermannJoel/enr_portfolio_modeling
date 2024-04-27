# -*- coding: utf-8 -*-
"""
Created on Sun Jun 26 17:52:09 2022

@author: hermann.ngayap
"""
import sys
import os
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from dash import dcc, html
import plotly.graph_objs as go
from dashboards.graphs.merchant_year_graph import merchant_year_bar
from dashboards.graphs.merchant_qtr_graph import merchant_qtr_bar
from dashboards.graphs.merchant_mth_graph import merchant_mth_bar


merchant_cr_layout=html.Div(
    children=[
            html.Div(
            className="central-panel1-title",
            children=["Prod Merchant"],
            ),
            
            html.Div(
           className="central-panel1",
           children=[
               html.Div(
                   children=[
                       html.Div(
                           style={
                               "display": "inline-block",
                               "vertical-align": "top",
                               "width": "70%",
                           },
                           children=[
                               merchant_year_bar,
                               merchant_qtr_bar,
                               merchant_mth_bar
                               ]
                           )
                       ]
                   )]
           )],
    )



