# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 11:33:04 2022

@author: hermann.ngayap
"""
from dash import html
from dashboards.graphs.solar_wind_power_production_graph import prod_solar_wind_power_gr
from dashboards.graphs.solar_wind_power_exposure_graph import exposure_solar_wind_power_gr
from dashboards.graphs.solar_wind_power_prod_hedge_exp_graph import prod_hedge_exp_solar_wind_power_gr
from dashboards.graphs.wind_power_prod_hedge_exp_graph import prod_hedge_exp_wind_power_gr
   
solar_wind_power_prod_layout=html.Div(
        children=[
            html.Div(
                style={
                    "display": "inline-block",
                    "vertical-align": "top",
                    "width": "25%",
                    },
                children=[ 
                    prod_solar_wind_power_gr
                      ],
                ),
            html.Div(
                style={
                    "display": "inline-block",
                    "margin-top": "0px",
                    "width": "25%",
                    },
                children=[
                    prod_hedge_exp_solar_wind_power_gr
                    ],
                ),
            html.Div(
                style={
                    "display": "inline-block",
                    "margin-top": "0px",
                    "width": "25%",
                    },
                children=[
                    prod_hedge_exp_wind_power_gr
                    ],
                ),           
            html.Div(
                style={
                    "display": "inline-block",
                    "vertical-align": "top",
                    "width": "25%",
                    }, 
                children=[
                    exposure_solar_wind_power_gr
                    ],
                ),
            ],
        )