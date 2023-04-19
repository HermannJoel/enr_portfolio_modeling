# -*- coding: utf-8 -*-
"""
Created on Fri Jun  3 15:06:59 2022
@author: hermann.ngayap

"""
import plotly.graph_objs as go
import dash_core_components as dcc
import dash
import dash_html_components as html
from dash.dependencies import Output, Input
import plotly.io as pio
import os
pio.renderers.default='browser'
from colors import colors
from x_axes import years, quarters, months 
from postgresql_queries import*

from tabs.MtM_tab import MtM_layout
from tabs.merchant_cr_tab import merchant_cr_layout
from tabs.prod_hedge_exp_tab import prod_hedge_exp_layout
from tabs.solar_wind_power_tab import solar_wind_power_prod_layout
import dash_auth
import os
import plotly.express as px
import psycopg2
import urllib.parse

username_password_pairs = [['username', 'password'], ['blx_mdp', '04042O22']]


#********** Dash App
width=1
dashed="solid"
BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs
#********** Start
app = dash.Dash(suppress_callback_exceptions=True)
auth = dash_auth.BasicAuth(app, username_password_pairs)
server = app.server

year_count = []
for year in years['years'].unique():
    year_count.append({'label':str(year),'value':year})

   
#********** Define tab_selected_style. Unfortunately cannot be defined in .css files

tab_height = 40
tab_style = {"height": tab_height, "line-height": tab_height, "padding": 0}
tab_selected_style = {
    "backgroundColor": colors["background3"],
    "color": colors["white"],
    "height": tab_height,
    "line-height": tab_height,
    "padding": 0,
    "font-weight": "bold",
}

app.layout = html.Div(
    className="screen-filler",
    children=[
           
        html.Div(
            style={
                "border-color": colors["darkgrey"],
                "border-style": "none none solid solid",
                "border-width": "1px",
                "padding-bottom": "0.2%",
                "padding-left": "1.75%",
                "padding-right": "1.75%",
                "padding-top": "0.1%",
                },

            children=[
                html.H1(
                    children="BORALEX'S PORTFOLIO", style={"font-size": 18, 'textAlign': 'center', 'color': colors['white']}
                        ),
                
                html.Div([
                   html.Img(src=app.get_asset_url('images/boralex_2.png'),
                            id='boralex_logo',
                            style={
                                "height": "5%",
                                "width": "5%",
                                "margin-bottom": "25px",
                                'textAlign': 'center'
                            },)
               ],
                   className="one-third column",
               ),
                ]
            ),

        html.Div( 
            style={ 
                "padding-top": "5px"
                },
            children=[
                html.Div(
                    children=[
                        html.Div(
                          
                           className="central-panel0",
                           children=[
                               html.Div(
                                  
                                   className="central-panel1-title",
                                   children=["PORTFOLIO MANAGEMENT STRATEGIES"]   
                               ),
                               
                               html.Div(
                                   className="central-panel1",
                                   children=[
                                       #
                                       dcc.Loading(
                                           children=[
                                               dcc.Tabs(
                                               id="tabsID",
                                               className="custom-tabs",
                                                colors={
                                                    "background": "#234253",
                                                    "border": "#3C3F47",
                                                    "primary": colors[
                                                        "solar_blx"
                                                    ],
                                                },
                                                style={
                                                    "font-size": 12,
                                                    "height": 50,
                                                },
                                                children=[
                                                    dcc.Tab(
                                                        children = [
                                                            prod_hedge_exp_layout
                                                            ],
                                                        className="custom-tab",
                                                        label="Production, Hedge & Exposure",
                                                        selected_style=tab_selected_style,
                                                        style=tab_style,
                                                        ),
                                                    dcc.Tab(
                                                        children = [
                                                            solar_wind_power_prod_layout
                                                            ],
                                                        className="custom-tab",
                                                        label="Production, Hedge, Exposure /Solar & Wind Power",
                                                        selected_style=tab_selected_style,
                                                        style=tab_style,
                                                        ),
                                                    dcc.Tab(
                                                        children=[
                                                                merchant_cr_layout
                                                            ],
                                                        className="custom-tab",
                                                            label="Production Merchant vs PPA & Coverage Ratio",
                                                            selected_style=tab_selected_style,
                                                            style=tab_style,
                                                        ),
                                                    
                                                    dcc.Tab(
                                                        children=[
                                                                MtM_layout
                                                            ],
                                                        className="custom-tab",
                                                            label="Mark to Market",
                                                            selected_style=tab_selected_style,
                                                            style=tab_style,
                                                        ),
                                                    
                                                   ],
                                                )
                                               
                                               ],
                                           )
                                       ],
                                   ),
                              ] 
                           ),
                        ]
                    ),
                ],
            ),
        ],
    )


#**********Exposure per quarter callback  
@app.callback(Output('exposition_q', 'figure'),
              [Input('drop_year_q', 'value')])
def update_figure_q(selected_year_q):
    filtered_df_q = query_results_2[query_results_2['year'] == selected_year_q]
    qtr = []
    for quarter in filtered_df_q['quarters'].unique():
        df_by_quarter = filtered_df_q[filtered_df_q['quarters'] == quarter]
        qtr.append(go.Bar(
            name='exposure',
            x=df_by_quarter['quarters'],
            y=df_by_quarter['exposure'],
            marker=dict(color=colors['e_white']),
            opacity=0.20
        ))

    return {
        'data': qtr,
        'layout': go.Layout(
            title='Exposure/Quarter/Year',
            xaxis=dict(gridcolor=colors['grid'], title='quarter', dtick=1),
            yaxis=dict(gridcolor=colors['grid'], title='GWh'),
            showlegend = False,
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            
        )
    }

#**********Exposure per month callback

@app.callback(Output('exposition_m', 'figure'),
              [Input('drop_year_m', 'value')])

def update_figure_m(selected_year_m):
    filtered_df_m = query_results_3[query_results_3['year'] == selected_year_m]
    mth = []
    for month in filtered_df_m['months'].unique():
        df_by_month = filtered_df_m[filtered_df_m['months'] == month]
        mth.append(go.Bar(
            name='Exposure',
            x=df_by_month['months'],
            y=df_by_month['exposure'],
            marker=dict(color=colors['e_white']),
            opacity=0.20
               
        ))

    return {
        'data': mth,
        'layout': go.Layout(title='Exposure/Month/Year',
            xaxis=dict(gridcolor=colors['grid'], title='months', dtick=1, tickangle = 45),
            yaxis=dict(gridcolor=colors['grid'], title= 'GWh'),
            showlegend = False,
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            
        )
    }

#**********Prod per quarter callback
@app.callback(Output('prod_q', 'figure'),
              [Input('drop_year_p_q', 'value')])

def update_figure_p_q(selected_year_p_q):
    filtered_df_p_q = query_results_11[query_results_11['year'] == selected_year_p_q]
    qtr_p = []
    for quarter in filtered_df_p_q['quarters'].unique():
        df_p_by_quarter = filtered_df_p_q[filtered_df_p_q['quarters'] == quarter]
        qtr_p.append(go.Bar(
            name='Production',
            x=df_p_by_quarter['quarters'],
            y=df_p_by_quarter['prod'],
            marker=dict(color=colors['l_green'])
            
        ))

    return {
        'data': qtr_p,
        'layout': go.Layout(title='Prod/Quarter/Year',
            xaxis=dict(gridcolor=colors['grid'], title='quarter', dtick=1),
            yaxis=dict(gridcolor=colors['grid'], title= 'GWh'),
            showlegend = False,
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    }
#**********Prod per month callback
@app.callback(Output('prod_m', 'figure'),
              [Input('drop_year_p_m', 'value')])

def update_figure_p_m(selected_year_p_m):
    filtered_df_p_m = query_results_12[query_results_12['year'] == selected_year_p_m]
    mth_p = []
    for month in filtered_df_p_m['months'].unique():
        df_p_by_month = filtered_df_p_m[filtered_df_p_m['months'] == month]
        mth_p.append(go.Bar(
            name='Production',
            x=df_p_by_month['months'],
            y=df_p_by_month['prod'],
            marker=dict(color=colors['l_green']),
               
        ))

    return {
        'data': mth_p,
        'layout': go.Layout(title='Prod/Month/Year',
            xaxis=dict(gridcolor=colors['grid'], title='months', dtick=1, tickangle = 45),
            yaxis=dict(gridcolor=colors['grid'], title= 'GWh'),
            showlegend = False,
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            
        )
    }

#**********Hedge per quarter callback

@app.callback(Output('hedge_type_q', 'figure'),
              [Input('drop_year_h_q', 'value')])

def update_figure_h_q(selected_year_h_q):
    filtered_df_h_q = query_results_5[query_results_5['year'] == selected_year_h_q]
    filtered_df_p_q_ = query_results_11[query_results_11['year'] == selected_year_h_q]
    qtr_h_ppa = []
    qtr_h_oa = []
    qtr_h_cr = []
    for quarter in filtered_df_h_q['quarters'].unique():
        df_h_by_quarter = filtered_df_h_q[filtered_df_h_q['quarters'] == quarter]
        qtr_h_ppa.append(go.Bar(
            name='PPA',
            x=quarters,
            y=df_h_by_quarter.loc[df_h_by_quarter['typecontract']=='PPA', 'hedge']),
            opacity=1,
            marker=dict(color=colors['ppa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ),
        qtr_h_oa.append(go.Bar(
            name='OA',
            x=quarters,
            y=df_h_by_quarter.loc[df_h_by_quarter['typecontract']=='OA', 'hedge']),
            opacity=0.4,
            base=df_h_by_quarter.loc[df_h_by_quarter['typecontract']=='PPA', 'hedge'],
            marker=dict(color=colors['oa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ),
        qtr_h_cr.append(go.Bar(
            name='CR',
            x=quarters,
            y=df_h_by_quarter.loc[df_h_by_quarter['typecontract']=='CR', 'hedge']),
            opacity=0.25,
            base=df_h_by_quarter.loc[df_h_by_quarter['typecontract']=='OA', 'hedge'],
            marker=dict(color=colors['cr']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ),

    for quarter in filtered_df_p_q_['quarters'].unique():
        df_p_by_quarter_ = filtered_df_p_q_[filtered_df_p_q_['quarters'] == quarter]
        qtr_p = []
        qtr_p.append(go.Bar(
            name='Production',
            x=quarters,
            y=df_p_by_quarter_['prod'],
            marker=dict(color=colors['l_green']),
            opacity=0.1,
            
            ))
            
    return {
        'data': (qtr_h_ppa, qtr_h_oa, qtr_h_cr),
        'layout': go.Layout(title='',
            annotations=annotations,
            xaxis=dict(gridcolor=colors['grid'], title='quarter', dtick=1),
            yaxis=dict(gridcolor=colors['grid'], title= 'GWh', side='left'),
            showlegend = True,
            barmode = "overlay",
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
        )
    }

#**********Hedge per month callback

@app.callback(Output('hedge_type_m', 'figure'),
              [Input('drop_year_h_m', 'value')])

def update_figure_h_m(selected_year_h_m):
    filtered_df_h_m = query_results_6[query_results_6['year'] == selected_year_h_m]
    filtered_df_p_m_ = query_results_12[query_results_12['year'] == selected_year_h_m]
    mth_h_ppa = []
    mth_h_oa = []
    mth_h_cr = []
    for month in filtered_df_h_m['months'].unique():
        df_h_by_month = filtered_df_h_m[filtered_df_h_m['months'] == month]
        mth_h_ppa.append(go.Bar(
            x=months['months'],
            y=df_h_by_month.loc[df_h_by_month['typecontract']=='PPA', 'hedge'],
            opacity=1,
            marker=dict(color=colors['ppa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            )),
        mth_h_cr.append(go.Bar(
            name="CR",
            x=months['months'],
            y=df_h_by_month.loc[df_h_by_month['typecontract'] == 'CR', 'hedge'],
            opacity=0.25,
            base=df_h_by_month.loc[df_h_by_month['typecontract'] == 'OA', 'hedge'],
            marker=dict(color=colors['cr']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            )),
        mth_h_oa.append(go.Bar(
            name="0A",   
            x=months['months'],
            y=df_h_by_month.loc[df_h_by_month['typecontract'] == 'OA', 'hedge'],
            opacity=0.4,
            base=df_h_by_month.loc[df_h_by_month['typecontract'] == 'PPA', 'hedge'],
            marker=dict(color=colors['oa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            )),

    for month in filtered_df_p_m_['months'].unique():
         df_p_by_month_ = filtered_df_p_m_[filtered_df_p_m_['months'] == month]
         mth_p = []
         mth_p.append(go.Bar(
             name='Production',
             x=months['months'],
             y=df_p_by_month_['prod'],
             opacity=0.1,
             marker=dict(color=colors['e_white']),
             marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
             ))
    return {
        'data': (mth_h_ppa, mth_h_oa, mth_h_cr),
        'layout': go.Layout(title='',
            xaxis=dict(gridcolor=colors['grid'], title='months', dtick=1, tickangle = 45),
            yaxis=dict(gridcolor=colors['grid'], title= 'GWh'),
            showlegend=False,
            barmode = "overlay",
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
        )
    }

#**********
@app.callback(Output('merchant_ppa_mth', 'figure'),
              [Input('drop_year_m_ppa_m', 'value')])

def update_figure_m_ppa_m(selected_year_m_ppa_m):
    filtered_df_m_ppa_m = query_results_18[query_results_18['year'] == selected_year_m_ppa_m]
    filtered_df_not_oa_cr_m = query_results_21[query_results_21['year'] == selected_year_m_ppa_m]
    mth_m_ppa = []
    mth_not_oa_cr = []
    for month in filtered_df_m_ppa_m['months'].unique():
        df_m_ppa_by_mth = filtered_df_m_ppa_m[filtered_df_m_ppa_m['months'] == month]
        mth_m_ppa.append(go.Bar(
            x=months,
            y=df_m_ppa_by_mth['ppa'],
            opacity=0.2,
            marker=dict(color=colors['ppa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ))
        mth_not_oa_cr.append(go.Bar(
             x=months,
             y=df_h_by_quarter['typecontract'],
             opacity=1,
             marker=dict(color=colors['oa']),
             marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']))),
    for month in filtered_df_not_oa_cr_m['months'].unique():
        df_not_oa_cr_by_mth = filtered_df_not_oa_cr_m[filtered_df_not_oa_cr_m['months'] == month]
        mth_not_oa_cr.append(go.Bar(
            x=months,
            y=df_not_oa_cr_by_mth['prodmerchant'],
            opacity=0.4,
            marker=dict(color=colors['l_green'])),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']))
            
    return {
        'data': (),
        'layout': go.Layout(title='Hedge/contract type/month',
            xaxis=dict(gridcolor=colors['grid'], title='quarter', dtick=1),
            yaxis=dict(gridcolor=colors['grid'], title= 'GWh', side='left'),
            yaxis2=dict(gridcolor=colors['grid'], title= 'GWh', side='right', showline=True),
            showlegend = True,
            barmode = "overlay",
            paper_bgcolor = colors["background1"],
            plot_bgcolor= colors["background1"],
            font=dict(color=colors["text"], size=PLOTS_FONT_SIZE)
        )
    }


if __name__ == '__main__':
    app.run_server(host='127.0.0.1', port=8050, debug=True) 
