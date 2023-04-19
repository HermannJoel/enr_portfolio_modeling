# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 14:13:07 2022

@author: hermann.ngayap
"""
import plotly.graph_objs as go
from colors import colors
from dash import dcc, html
from x_axes import years, quarters, months
from postgresql_queries import*


BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in years['years'].unique():
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
        ) for xi, yi, zi in zip(months['months'], query_results_21['prodmerchant'], query_results_24['hcr'])]
    
merchant_mth_bar=html.Div(
    children=[
        #Dropdown hedge with ppa/month
        dcc.Dropdown(id='drop_year_m_ppa_m', options=year_count, value=years['years'].min(),
        style=dict(width='40%',verticalAlign="center", display='inline-block')),
        #Merchant hedge with ppa/month 
        dcc.Graph(id="m_ppa_cr_mth",
              figure = {'data':[
                  go.Bar(
                          name='HCR', 
                          x=months['months'], 
                          y=query_results_24['hcr'],
                          opacity=0,
                          marker=dict(color=colors['white']),
                          marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                          textposition='outside',
                          textfont = dict(family="Times", size= 10, color= colors["white"]),
                          ),
                  go.Bar(
                      name='PPA', 
                      x=months['months'], 
                      y=query_results_18['ppa'],
                      opacity=1,
                      marker=dict(color=colors['ppa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                     name='Prod Merchant', 
                     x=months['months'], 
                     y=query_results_21['prodmerchant'],
                     opacity=0.25,
                     marker=dict(color=colors['e_white']),                             
                     marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']) 
                     ), 
                 ], 
                  'layout':go.Layout(title='Prod Merchant Hedged with PPA/Month',
                                     annotations=annotations,
                                     xaxis=dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                     yaxis=dict(gridcolor=colors['grid'], title='GWh', side='left'),
                                     yaxis_range=[0,25],
                                     height=500,
                                     barmode='overlay',
                                     paper_bgcolor = colors["background1"],
                                     plot_bgcolor= colors["background1"],
                                     font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                     showlegend=True,
                                     legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                     hovermode="x unified")
                  },
              style={'width': '65%', 'display': 'inline-block', 'vertical-align': 'top'},
              ),
        
        ]
    )
