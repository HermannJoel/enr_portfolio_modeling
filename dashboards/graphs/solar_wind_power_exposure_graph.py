# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 11:17:00 2022

@author: hermann.ngayap
"""
import sys
import os
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
# import dash_core_components as dcc
# import dash_html_components as html
from dash import dcc, html
import plotly.graph_objs as go
from dashboards.env import*  
from queries.pg_dwh_queries import*

BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})

exposure_solar_wind_power_gr = html.Div(
    children=[
        html.H2(
            children="Market Exposure",
            style={
                "font-size": 14,
                "margin-bottom": "0em",
                "margin-top": "1em",
                },
            ),
        #Exposition per year
        dcc.Graph(id='sol_wp_exp_y',
                  figure = {'data':[
                      go.Bar(
                          name='Solar',
                          x=years['years'],
                          y=exposure_sol_y["ExposureSolar"],
                          marker=dict(color=colors['e_white']),
                          opacity=0.15
                          ),
                      go.Bar(
                          name='Wind Power',
                          x=years['years'],
                          y=exposure_wp_y["ExposureWp"],
                          marker=dict(color=colors['e_white_1']),
                          opacity=0.45)
                      ], 
                      'layout':go.Layout(dict(title='Solar Wind-Power Exposure/Year', 
                                              xaxis = dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                              yaxis = dict(gridcolor=colors['grid'], title='GWh'),
                                              paper_bgcolor = colors["background1"],
                                              plot_bgcolor= colors["background1"],
                                              font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                              showlegend=True,
                                              legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                              hovermode="x unified"
                                              ))
                      },
                  style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'},
                  ),
        dcc.Dropdown(id='sol_wp_drop_year_q',options=year_count,value=years['years'].min(),
                     style=dict(width='45%',verticalAlign="left", display='inline-block', )),
        #Exposition per quarter
        dcc.Graph(id='sol_wp_exp_q', 
                  figure = {'data':[
                      go.Bar(
                          name='Solar',
                          x=quarters['quarters'],
                          y=exposure_sol_q["ExposureSolar"],
                          marker=dict(color=colors['e_white']),
                          opacity=0.15
                          ),
                      go.Bar(
                          name='Wind Power',
                          x=quarters['quarters'],
                          y=exposure_wp_q["ExposureWp"],
                          marker=dict(color=colors['e_white_1']),
                          opacity=0.45
                          )
                      ], 
                      'layout':go.Layout(title='Solar Wind-Power Exposure/Quarter/Year', 
                                         xaxis = dict(gridcolor=colors['grid'], title='quarter'), 
                                         yaxis = dict(gridcolor=colors['grid'], title='GWh'),
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         hovermode="x unified",
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                         )
                      },
                  style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'} 
                  ),
        #Dropdown exposition per quarter
        dcc.Dropdown(id='sol_wp_drop_year_m',options=year_count,value=years['years'].min(), 
                     style=dict(width='45%', verticalAlign="right", display='inline-block')),
        #Exposition per month
        dcc.Graph(id='sol_wp_exp_m',
                  figure = {'data':[
                      go.Bar(
                          name='Solar',
                          x=months['months'],
                          y=exposure_sol_m["ExposureSolar"],
                          marker=dict(color=colors['e_white']),
                          opacity=0.15
                          ),
                      go.Bar(
                          name='Wind Power',
                          x=months['months'],
                          y=exposure_wp_m["ExposureWp"],
                          marker=dict(color=colors['e_white_1']),
                          opacity=0.45
                          )
                      ], 
                      'layout':go.Layout(title='Solar Wind-Power Exposure/Month/Year', 
                                         xaxis = dict(gridcolor=colors['grid'], title='months', tickangle = 45), 
                                         yaxis=dict(gridcolor=colors['grid'], title='GWh'),
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         hovermode="x unified",
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                         ),
                      }, 
                  style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}
                  ),
        
        ]
    )