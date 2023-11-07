# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 18:19:33 2022

@author: hermann.ngayap
"""

from dash import dcc, html
import plotly.graph_objs as go
from x_axes import years, quarters, months
from colors import colors 
from postgresql_queries import*

width=1.5
dashed="solid"
BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

MtM_sp=html.Div(
    children=[
        dcc.Graph(id="mtm_scatter_p",
                  figure = {'data':[
                       go.Scatter(
                          name='MtM', 
                          x=years['years'], 
                          y=query_results_49['mtm'],
                          line=dict(color=colors['mtm_y'], dash=dashed, width=width),
                          mode='markers+lines',
                          marker=dict(color=colors['white'], size = 1, symbol = 'pentagon', 
                                      line = dict(width=2))
                          ),
                       go.Scatter(
                          name='MtM-Merchant', 
                          x=years['years'], 
                          y=query_results_50['mtm'],
                          mode='markers+lines',
                          line=dict(color=colors['mtm_q'], dash=dashed, width=width),
                          marker=dict(color=colors['white'], size=1, symbol='pentagon', 
                                      line = dict(width=1)),
                              ),
                       go.Scatter(
                          name='MtM-Reguled', 
                          x=years['years'], 
                          y=query_results_51['mtm'],
                          line=dict(color=colors['mtm_m'], dash=dashed, width=width),
                          mode='markers+lines',
                          marker=dict(color=colors['white'], size=1, symbol='pentagon', 
                                      line=dict(width=1)),
                              ), 
                     ], 
                      'layout':go.Layout(title='MtM/Years',
                                         xaxis=dict(gridcolor=colors['grid'], title='Years', dtick=1, tickangle = 45), 
                                         yaxis=dict(gridcolor=colors['grid'], title='M€', side='left'),
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                         hovermode="x unified")}),
        ],

 )