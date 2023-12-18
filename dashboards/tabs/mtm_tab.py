# -*- coding: utf-8 -*-
"""
Created on Sun Jun 26 13:04:12 2022

@author: hermann.ngayap
"""
from dash import dcc, html
import plotly.graph_objs as go
from dashboards.graphs.mtm_graph import MtM_graph
from dashboards.graphs.mtm_historic_graph import MtM_H_graph
  

MtM_layout=html.Div(
    children=[
            html.Div(
            className="central-panel1-title",
            children=["MtM"],
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
                               "width": "100%",
                           },
                           children=[
                               MtM_graph,
                               MtM_H_graph
                               ]
                           ),
# =============================================================================
#                        html.Div(
#                            style={
#                                "display": "inline-block",
#                                "margin-top": "0px",
#                                "width": "25%",
#                                },
#                            children=[
#                              
#                                ],
#                            ),
# =============================================================================
                       ]
                   )]
           )],
    )