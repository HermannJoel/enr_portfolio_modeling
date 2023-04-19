# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 12:05:34 2022

@author: hermann.ngayap
"""
from dash import dcc, html
import plotly.graph_objs as go
from colors import colors
from tables.prod_merchant_table import prod_merchant_tbl
from functions import make_dbc_table
from x_axes import years, quarters, months 
from postgresql_queries import*

annotations = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(years['years'], query_results_19['prodmerchant'], query_results_22['hcr'])]

BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})

merchant_year_bar= html.Div(
    children=[
        html.Div(
            className="central-panel1",
            children=[
                html.Div(
                    children=[
                        html.Div(
                            style={
                                "display": "inline-block",
                                "vertical-align": "top",
                                "width": "50%",
                            },
                            children=[
                                dcc.Graph(id="m_ppa_cr_y",
                                          figure = {'data':[
                                               go.Bar(
                                                  name='HCR', 
                                                  x=years['years'], 
                                                  y=query_results_22['hcr'],
                                                  opacity=0,
                                                  marker=dict(color=colors['white']),
                                                  marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                                                  textposition='outside',
                                                  textfont = dict(family="Times", size= 10, color= colors["white"]),
                                                      ),
                                               go.Bar(
                                                  name='PPA', 
                                                  x=years['years'], 
                                                  y=query_results_16['ppa'],
                                                  opacity=1,
                                                  marker=dict(color=colors['ppa']),
                                                  marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                                                  ),
                                               go.Bar(
                                                 name='Prod Merchant', 
                                                 x=years['years'], 
                                                 y=query_results_19['prodmerchant'],
                                                 opacity=0.25,
                                                 marker=dict(color=colors['e_white']),                             
                                                 marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']) 
                                                 ), 
                                             ], 
                                              'layout':go.Layout(title='Prod Merchant Hedged with PPA/year',
                                                                 annotations=annotations,
                                                                 xaxis=dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                                                 yaxis=dict(gridcolor=colors['grid'], title='GWh', side='left'),
                                                                 barmode='overlay',
                                                                 paper_bgcolor = colors["background1"],
                                                                 plot_bgcolor= colors["background1"],
                                                                 font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                                                 showlegend=True,
                                                                 legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                                                 hovermode="x unified")}),
                                ],
                        ),
                        html.Div(
                            style={
                                "display": "inline-block",
                                "margin-top": "0px",
                                "margin-left": "25px",
                                "width": "25%",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        prod_merchant_tbl
                                        ],
                                    className="table",
                                )
                            ],
                        ),
                    ]
                ),

            ],
        ),
    ],
    id="container_prod_fc",
)