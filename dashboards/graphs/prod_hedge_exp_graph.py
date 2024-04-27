# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 09:41:36 2022

@author: hermann.ngayap
"""
import sys
import os
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from dash import dcc, html
import plotly.graph_objs as go
import plotly.express as px
from dashboards.env import*  
from queries.pg_dwh_queries import*

BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})

annotations_y = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(years["years"], prod_y["Prod"], hcr_y["HCR"])]
    
annotations_q = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(quarters["quarters"], prod_q["Prod"], hcr_q["HCR"])]

annotations_m = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(months["months"], prod_m["Prod"], hcr_m["HCR"])]

    
prod_hedge_exp_graph=html.Div(
    
    children=[
    html.H2(
        children="Production/Hedge/Exposure",
        style={
            "font-size": 14,
            "margin-bottom": "0em",
            "margin-top": "1em",
            },
        ),
    #Hedge per year
    dcc.Graph(id='hedge_type_y',
               figure = {'data':[
                   go.Bar(
                       name='HCR', 
                       x=years["years"], 
                       y=hcr_y["HCR"],
                       opacity=0,
                       marker=dict(color=colors['white']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                       ),
                   go.Bar(
                       name='PPA', 
                       x=years['years'], 
                       y=hedge_y.loc[hedge_y["TypeContract"] == 'PPA', "Hedge"],
                       opacity=1,
                       marker=dict(color=colors['ppa']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                       ),
                   go.Bar(
                       name='OA', 
                       x=years['years'], 
                       y=hedge_y.loc[hedge_y["TypeContract"] == 'OA', "Hedge"],
                       opacity=0.4,
                       base=hedge_y.loc[hedge_y["TypeContract"] == 'PPA', "Hedge"],
                       marker=dict(color=colors['oa']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                       ),
                   go.Bar(
                       name='CR', 
                       x=years['years'], 
                       y=hedge_y.loc[hedge_y["TypeContract"] == 'CR', "Hedge"],
                       opacity=0.25,
                       base=hedge_y.loc[hedge_y["TypeContract"] == 'OA', "Hedge"],
                       marker=dict(color=colors['cr']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                       ),

                  go.Bar(
                      name='Production', 
                      x=years['years'], 
                      y=prod_y["Prod"],
                      opacity=0.09,
                      marker=dict(color=colors['e_white']),                             
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']) 
                      ),
                 ], 
                    'layout':go.Layout(title='',
                                      annotations=annotations_y,
                                      xaxis=dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                      yaxis=dict(gridcolor=colors['grid'], title='GWh', side='left'),
                                      barmode='overlay',
                                      paper_bgcolor = colors["background1"],
                                      plot_bgcolor= colors["background1"],
                                      font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                      showlegend=True,
                                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                      hovermode="x unified")
                  },
              style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'},
              ),
    #Dropdown Hedge per quarter
    dcc.Dropdown(id='drop_year_h_q', options=year_count, value=years['years'].min(), 
                 style=dict(width='40%', verticalAlign="left", display='inline-block')),
    #Hedge per quarter
    dcc.Graph(id='hedge_type_q',
              figure = {'data':[
 
                 go.Bar(
                      name='HCR', 
                      x=quarters['quarters'], 
                      y=hcr_q["HCR"],
                      opacity=0,
                      marker=dict(color=colors['white']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                      textposition = "outside",
                      textfont = dict(family="Times", size= 10, color= colors["white"]),
                      ), 
                  go.Bar(
                      name='PPA',
                      x=quarters['quarters'],
                      y=hedge_q.loc[hedge_q["TypeContract"]=='PPA', "Hedge"],
                      opacity=1,
                      marker=dict(color=colors['ppa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name='OA',
                      x=quarters['quarters'],
                      y=hedge_q.loc[hedge_q["TypeContract"]=='OA', "Hedge"],
                      opacity=0.4,
                      base=hedge_q.loc[hedge_q["TypeContract"]=='PPA', "Hedge"],
                      marker=dict(color=colors['oa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name='CR',
                      x=quarters['quarters'],
                      y=hedge_q.loc[hedge_q["TypeContract"]=='CR', "Hedge"],
                      opacity=0.25,
                      base=hedge_q.loc[hedge_q["TypeContract"]=='OA', "Hedge"],
                      marker=dict(color=colors['cr']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
 
                  go.Bar(
                      name='Production', 
                      x=quarters['quarters'],
                      y=prod_q["Prod"],
                      opacity=0.1,
                      marker=dict(color=colors['e_white']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),

                     ),
                  ],
                  'layout':go.Layout(title='',
                                     annotations=annotations_q,
                                     xaxis=dict(gridcolor=colors['grid'], title='quarter'), 
                                     yaxis=dict(gridcolor=colors['grid'], title ='GWh', side='left'),
                                     barmode='overlay',
                                     paper_bgcolor = colors["background1"],
                                     plot_bgcolor= colors["background1"],
                                     font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                     showlegend=True,
                                     legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                     hovermode="x unified",
                                     ),
                  }, 
              style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'},
              ),
 
    #Dropdown Hedge per month
    dcc.Dropdown(id='drop_year_h_m',options=year_count, value=years['years'].min(), 
         style=dict(width='40%', verticalAlign="left", display='inline-block')),
    #Hedge per month
    dcc.Graph(id='hedge_type_m',
              figure = {'data':[
                  go.Bar(
                       name='HCR', 
                       x=months['months'], 
                       y=hcr_m["HCR"],
                       opacity=0,
                       marker=dict(color=colors['white']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                       textposition = "outside",
                       textfont = dict(family="Times", size= 10, color= colors["white"]),
                       ), 
                  go.Bar(
                      name="0A",   
                      x=months['months'],
                      y=hedge_m.loc[hedge_m["TypeContract"] == 'OA', "Hedge"],
                      opacity=0.4,
                      base=hedge_m.loc[hedge_m["TypeContract"] == 'PPA', "Hedge"],
                      marker=dict(color=colors['oa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name="CR",
                      x=months['months'],
                      y=hedge_m.loc[hedge_m["TypeContract"] == 'CR', "Hedge"],
                      opacity=0.25,
                      base=hedge_m.loc[hedge_m["TypeContract"] == 'OA', "Hedge"],
                      marker=dict(color=colors['cr']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ), 
                  go.Bar(
                      name="PPA",
                      x=months['months'],
                      y=hedge_m.loc[hedge_m["TypeContract"] == 'PPA', "Hedge"],
                      opacity=1,
                      marker=dict(color=colors['ppa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name='Production', 
                      x=months['months'],
                      y=prod_m["Prod"],
                      opacity=0.1,
                      marker=dict(color=colors['e_white']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),

                     ),
                  ],
                  
                  'layout':go.Layout(title='',
                                     annotations=annotations_m,
                                     xaxis = dict(gridcolor=colors['grid'], title='months', dtick=1, tickangle = 45), 
                                     yaxis= dict(gridcolor=colors['grid'], title= 'GWh'),
                                     barmode='overlay',
                                     paper_bgcolor = colors["background1"],
                                     plot_bgcolor= colors["background1"],
                                     font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                     showlegend=True,
                                     legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                     hovermode="x unified"
                                     )},
              style={'width': '100%', 'display': 'block', 'vertical-align': 'top'}
              ),
        
        ],
    )
        