# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 09:41:36 2022

@author: hermann.ngayap
"""
from dash import dcc, html
import plotly.graph_objs as go
from colors import colors
from x_axes import years, quarters, months 
from postgresql_queries import*
import plotly.express as px

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
        ) for xi, yi, zi in zip(years['years'], query_results_10['prod'], query_results_7['hcr'])]
    
annotations_q = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(quarters['quarters'], query_results_11['prod'], query_results_8['hcr'])]

annotations_m = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(months['months'], query_results_12['prod'], query_results_9['hcr'])]

    
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
                       x=years['years'], 
                       y=query_results_7['hcr'],
                       opacity=0,
                       marker=dict(color=colors['white']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                       ),
                   go.Bar(
                       name='PPA', 
                       x=years['years'], 
                       y=query_results_4.loc[query_results_4['typecontract'] == 'PPA', 'hedge'],
                       opacity=1,
                       marker=dict(color=colors['ppa']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                       ),
                   go.Bar(
                       name='OA', 
                       x=years['years'], 
                       y=query_results_4.loc[query_results_4['typecontract'] == 'OA', 'hedge'],
                       opacity=0.4,
                       base=query_results_4.loc[query_results_4['typecontract'] == 'PPA', 'hedge'],
                       marker=dict(color=colors['oa']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                       ),
                   go.Bar(
                       name='CR', 
                       x=years['years'], 
                       y=query_results_4.loc[query_results_4['typecontract'] == 'CR', 'hedge'],
                       opacity=0.25,
                       base=query_results_4.loc[query_results_4['typecontract'] == 'OA', 'hedge'],
                       marker=dict(color=colors['cr']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                       ),

                  go.Bar(
                      name='Production', 
                      x=years['years'], 
                      y=query_results_10['prod'],
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
                      y=query_results_8['hcr'],
                      opacity=0,
                      marker=dict(color=colors['white']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                      textposition = "outside",
                      textfont = dict(family="Times", size= 10, color= colors["white"]),
                      ), 
                  go.Bar(
                      name='PPA',
                      x=quarters['quarters'],
                      y=query_results_5.loc[query_results_5['typecontract']=='PPA', 'hedge'],
                      opacity=1,
                      marker=dict(color=colors['ppa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name='OA',
                      x=quarters['quarters'],
                      y=query_results_5.loc[query_results_5['typecontract']=='OA', 'hedge'],
                      opacity=0.4,
                      base=query_results_5.loc[query_results_5['typecontract']=='PPA', 'hedge'],
                      marker=dict(color=colors['oa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name='CR',
                      x=quarters['quarters'],
                      y=query_results_5.loc[query_results_5['typecontract']=='CR', 'hedge'],
                      opacity=0.25,
                      base=query_results_5.loc[query_results_5['typecontract']=='OA', 'hedge'],
                      marker=dict(color=colors['cr']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
 
                  go.Bar(
                      name='Production', 
                      x=quarters['quarters'],
                      y=query_results_11['prod'],
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
                       y=query_results_9['hcr'],
                       opacity=0,
                       marker=dict(color=colors['white']),
                       marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                       textposition = "outside",
                       textfont = dict(family="Times", size= 10, color= colors["white"]),
                       ), 
                  go.Bar(
                      name="0A",   
                      x=months['months'],
                      y=query_results_6.loc[query_results_6['typecontract'] == 'OA', 'hedge'],
                      opacity=0.4,
                      base=query_results_6.loc[query_results_6['typecontract'] == 'PPA', 'hedge'],
                      marker=dict(color=colors['oa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name="CR",
                      x=months['months'],
                      y=query_results_6.loc[query_results_6['typecontract'] == 'CR', 'hedge'],
                      opacity=0.25,
                      base=query_results_6.loc[query_results_6['typecontract'] == 'OA', 'hedge'],
                      marker=dict(color=colors['cr']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ), 
                  go.Bar(
                      name="PPA",
                      x=months['months'],
                      y=query_results_6.loc[query_results_6['typecontract'] == 'PPA', 'hedge'],
                      opacity=1,
                      marker=dict(color=colors['ppa']),
                      marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                      ),
                  go.Bar(
                      name='Production', 
                      x=months['months'],
                      y=query_results_12['prod'],
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
        