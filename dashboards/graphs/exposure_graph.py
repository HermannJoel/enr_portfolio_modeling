# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 09:42:15 2022

@author: hermann.ngayap
"""
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from x_axes import years, quarters, months 
from colors import colors
from postgresql_queries import*

BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})
    
    
exposure_graph=html.Div(
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
        dcc.Graph(id='exposition_y',
                  figure = {'data':[
                      go.Bar(
                          name='Exposure',
                          x=years['years'],
                          y=query_results_1['exposure'],
                          marker=dict(color=colors['e_white']),
                          opacity=0.15,
                          )], 
                      'layout':go.Layout(dict(title='Exposure/Year', 
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
        dcc.Dropdown(id='drop_year_q',options=year_count,value=years['years'].min(),
                     style=dict(width='40%',verticalAlign="left", display='inline-block')),
        #Exposition per quarter
        dcc.Graph(id='exposition_q', 
                  figure = {'data':[
                      go.Bar(
                          name='Exposure',
                          x=quarters['quarters'],
                          y=query_results_2['exposure'],
                          marker=dict(color=colors['e_white']),
                          opacity=0.20
                          )], 
                      'layout':go.Layout(title='Exposure/Quarter/Year', 
                                         xaxis = dict(gridcolor=colors['grid'], title='quarter'), 
                                         yaxis = dict(gridcolor=colors['grid'], title='GWh'),
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         hovermode="x unified",
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                                                     ),
                                         )
                      },
                  style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'} 
                  ),
        #Dropdown exposition per quarter
        dcc.Dropdown(id='drop_year_m',options=year_count,value=years['years'].min(), 
                     style=dict(width='40%', verticalAlign="right", display='inline-block')),
        #Exposition per month
        dcc.Graph(id='exposition_m',
                  figure = {'data':[
                      go.Bar(
                          name='Exposure',
                          x=months['months'],
                          y=query_results_3['exposure'],
                          marker=dict(color=colors['e_white']),
                          opacity=0.15
                          )], 
                      'layout':go.Layout(title='Exposure/Month/Year', 
                                         xaxis = dict(gridcolor=colors['grid'], title='months', tickangle = 45), 
                                         yaxis=dict(gridcolor=colors['grid'], title='GWh'),
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         hovermode="x unified",
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                                                     ),
                                         ),
                      }, 
                  style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}
                  ),
        ],
    )