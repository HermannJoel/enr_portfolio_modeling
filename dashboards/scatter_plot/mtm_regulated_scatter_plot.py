# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 18:16:23 2022

@author: hermann.ngayap
"""

from dash import dcc, html
import plotly.graph_objs as go
from x_axes import years, quarters, months
from colors import colors
from postgresql_queries import*

width=1
dashed="solid"
BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs


year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})

MtM_regulated_sp=html.Div(
    children=[
        
        dcc.Graph(id="mtm_reg_scatter_p_gr",
                  figure = {'data':[
                       go.Scatter(
                          name='MtM', 
                          x=years['years'], 
                          y=query_results_51['mtm'],
                          line=dict(color=colors['mtm_m'], dash=dashed, width=width),
                          mode='markers+lines',
                          marker=dict(color=colors['white'], size=1, symbol='pentagon', 
                                      line=dict(width=1)),
                              ), 
                     ], 
                      'layout':go.Layout(title='MtM Portfolio Reguled',
                                         xaxis=dict(gridcolor=colors['grid'], title='Years', visible= True, showticklabels= True, dtick=1, tickangle = 45), 
                                         yaxis=dict(gridcolor=colors['grid'], title='M€', side='left'),
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                         hovermode="x unified")},
                  ),

        ],

 )