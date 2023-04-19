# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 11:18:24 2022

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
for year in years['years']:
    year_count.append({'label':str(year),'value':year})

prod_solar_wind_power_gr = html.Div(
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
     dcc.Graph(id='sol_wp_prod_y',
               figure = {'data':[
                   go.Bar(
                       name="Solar",
                       x=years['years'],
                       y=query_results_25['prodsolar'],
                       marker=dict(color=colors['solar']),
                       ),
                   go.Bar(
                       name="Wind Power",
                       x=years['years'],
                       y=query_results_26['prodwp'],
                       marker=dict(color=colors['wind_power']),
                       )
                   ], 
                   'layout':go.Layout(dict(title='Prod Solar Wind-Power/Year', 
                                           xaxis = dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                           yaxis = dict(gridcolor=colors['grid'], title='GWh'),
                                           paper_bgcolor = colors["background1"],
                                           plot_bgcolor= colors["background1"],
                                           font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                           showlegend=True,
                                           legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                                                       ),
                                           hovermode="x unified"
                                           ))
                   },
               style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'},
               ),
     #Dropdown prod per quarter 
     dcc.Dropdown(id='sol_wp_drop_year_p_q',options=year_count,value=years['years'].min(),
                  style=dict(width='45%',verticalAlign="left", display='inline-block')),
     #prod per quarter
     dcc.Graph(id='sol_wp_prod_q', 
               figure = {'data':[
                   go.Bar(
                       name='Solar',
                       x=quarters['quarters'],
                       y=query_results_27['prodsolar'],
                       marker=dict(color=colors['solar']),
                       ),
                   go.Bar(
                       name='Wind Power',
                       x=quarters['quarters'],
                       y=query_results_28['prodwp'],
                       marker=dict(color=colors['wind_power']),
                       )
                   ], 
                   'layout':go.Layout(title='Prod Solar Wind-Power/Quarter/Year', 
                                      xaxis = dict(gridcolor=colors['grid'], title='quarter'), 
                                      yaxis = dict(gridcolor=colors['grid'], title='GWh'),
                                      paper_bgcolor = colors["background1"],
                                      plot_bgcolor= colors["background1"],
                                      font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                      showlegend=True,
                                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                                                  ),
                                      hovermode="x unified",
                                      )
                   },
               style={'width': '100%', 'display': 'block', 'vertical-align': 'top'} 
               ),
     #Dropdown prod per month
     dcc.Dropdown(id='sol_wp_drop_year_p_m',options=year_count,value=years['years'].min(), 
                  style=dict(width='45%', verticalAlign="left", display='inline-block')),
     #Prod per month
     dcc.Graph(id='sol_wp_prod_m',
               figure = {'data':[
                   go.Bar(
                       name='Solar',
                       x=months['months'],
                       y=query_results_29['prodsolar'],
                       marker=dict(color=colors['solar']),
                       ),
                   go.Bar(
                       name='Wind Power',
                       x=months['months'],
                       y=query_results_30['prodwp'],
                       marker=dict(color=colors['wind_power']),
                       )
                   ], 
                   'layout':go.Layout(title='Prod Solar Wind-Power/Month/Year', 
                                      xaxis = dict(gridcolor=colors['grid'], title='months', tickangle = 45), 
                                      yaxis=dict(gridcolor=colors['grid'], title='GWh'),
                                      paper_bgcolor = colors["background1"],
                                      plot_bgcolor= colors["background1"],
                                      font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                      showlegend=True,
                                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                                                  ),
                                      hovermode="x unified"
                                      )
                   }, 
               style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}
               ),
     ],
    )