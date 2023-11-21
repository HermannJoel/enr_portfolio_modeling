# -*- coding: utf-8 -*-
"""
Created on Sat Jul 16 17:00:20 2022

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

annotations = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(years['years'], query_results_26['prodwp'], query_results_44['hcrwp'])]

prod_hedge_exp_wind_power_gr = html.Div(
    children=[
        html.H2(
            children="Wind Power Production/Hedge/Exposure",
            style={
                "font-size": 14,
                "margin-bottom": "0em",
                "margin-top": "1em",
                },
            ),
        #Hedge per year
        dcc.Graph(id='wp_hedge_type_y',
                  figure = {'data':[
     
                      go.Bar(
                          name='HCR', 
                          x=years['years'], 
                          y=query_results_44['hcrwp'],
                          opacity=0.0,
                          marker=dict(color=colors['white']),
                          ),
                     
                      go.Bar(
                          name='PPA', 
                          x=years['years'], 
                          y=query_results_38.loc[query_results_38['typecontract'] == 'PPA', 'hedgewp'],
                          opacity=1,
                          marker=dict(color=colors['ppa']),
                          marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                          ),
                      
                      go.Bar(
                          name='OA', 
                          x=years['years'], 
                          y=query_results_38.loc[query_results_38['typecontract'] == 'OA', 'hedgewp'],
                          opacity=0.4,
                          base=query_results_38.loc[query_results_38['typecontract'] == 'PPA', 'hedgewp'],
                          marker=dict(color=colors['oa']),
                          marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                          ),

                      go.Bar(
                          name='CR', 
                          x=years['years'], 
                          y=query_results_38.loc[query_results_38['typecontract'] == 'CR', 'hedgewp'],
                          opacity=0.25,
                          base=query_results_38.loc[query_results_38['typecontract'] == 'OA', 'hedgewp'],
                          marker=dict(color=colors['cr']),
                          marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                          ),

                      go.Bar(
                          name='Prod Wind Power', 
                          x=years['years'], 
                          y=query_results_26['prodwp'],
                          opacity=0.09,
                          marker=dict(color=colors['wind_power']),                             
                          marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']) 
                          ), 
                     ], 
                      'layout':go.Layout(title='',
                                         annotations=annotations,
                                         xaxis=dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                         yaxis=dict(gridcolor=colors['grid'], title='GWh', side='left'),
                                         barmode='overlay',
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                                                     ),
                                         hovermode="x unified")
                      },
                  style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'},
                  ),

        
        ]
    )
