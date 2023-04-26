# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 09:07:49 2022

@author: hermann.ngayap
"""

import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from colors import colors
from x_axes import years, quarters, months 
from postgresql_queries import*


BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})

production_graph= html.Div(
    children=[
    html.H2(
         children="Production",
         style={
             "font-size": 14,
             "margin-bottom": "0em",
             "margin-top": "1em",
             },
         ),
     #prod per year
     dcc.Graph(id='prod_y',
               figure = {'data':[
                   go.Bar(
                       name='Production',
                       x=years['years'],
                       y=query_results_10['prod'],
                       marker=dict(color=colors['l_green']),
                       )], 
                   'layout':go.Layout(dict(title='Prod/Year', 
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
     #Dropdown prod per quarter 
     dcc.Dropdown(id='drop_year_p_q',options=year_count,value=years['years'].min(),
                  style=dict(width='40%',verticalAlign="left", display='inline-block')),
     #prod per quarter
     dcc.Graph(id='prod_q', 
               figure = {'data':[
                   go.Bar(
                       name='Production',
                       x=quarters['quarters'], #quarters
                       y=query_results_11['prod'],
                       marker=dict(color=colors['l_green']),
                       )], 
                   'layout':go.Layout(title='Prod/Quarter/Year', 
                                      xaxis = dict(gridcolor=colors['grid'], title='quarter'), 
                                      yaxis = dict(gridcolor=colors['grid'], title='GWh'),
                                      paper_bgcolor = colors["background1"],
                                      plot_bgcolor= colors["background1"],
                                      font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                      showlegend=False,
                                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                      hovermode="x unified",
                                      )
                   },
               style={'width': '100%', 'display': 'block', 'vertical-align': 'top'} 
               ),
     #Dropdown prod per month
     dcc.Dropdown(id='drop_year_p_m',options=year_count,value=years['years'].min(), 
                  style=dict(width='40%', verticalAlign="left", display='inline-block')),
     #Prod per month
     dcc.Graph(id='prod_m',
               figure = {'data':[
                   go.Bar(
                       name='Production',
                       x=months['months'],
                       y=query_results_12['prod'],
                       marker=dict(color=colors['l_green']),
                       )], 
                   'layout':go.Layout(title='Prod/Month/Year', 
                                      xaxis = dict(gridcolor=colors['grid'], title='months', tickangle = 45), 
                                      yaxis=dict(gridcolor=colors['grid'], title='GWh'),
                                      paper_bgcolor = colors["background1"],
                                      plot_bgcolor= colors["background1"],
                                      font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                      showlegend=False,
                                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                      hovermode="x unified"
                                      )
                   }, 
               style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}
               ),
     ],
    )



