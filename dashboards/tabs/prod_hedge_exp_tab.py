# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 09:07:45 2022

@author: hermann.ngayap
"""
from dash import html
from graphs.production_graph import production_graph
from graphs.prod_hedge_exp_graph import prod_hedge_exp_graph
from graphs.exposure_graph import exposure_graph

   
prod_hedge_exp_layout=html.Div(
        children=[
            html.Div(
                style={
                    "display": "inline-block",
                    "vertical-align": "top",
                    "width": "33%",
                    },
                children=[ 
                    production_graph
                      ],
                ),
            html.Div(
                style={
                    "display": "inline-block",
                    "margin-top": "0px",
                    "width": "33%",
                    },
                children=[
                    prod_hedge_exp_graph
                    ],
                ),
            html.Div(
                style={
                    "display": "inline-block",
                    "vertical-align": "top",
                    "width": "33%",
                    }, 
                children=[
                    exposure_graph
                    ],
                ),
            ],
        )