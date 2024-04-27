import pandas as pd
import sys
import os
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
app = dash.Dash(suppress_callback_exceptions=True)
from dashboards.env import*  
from queries.pg_dwh_queries import*

app = dash.Dash(suppress_callback_exceptions=True)
#**********Exposure per quarter callback  
@app.callback(Output('exposition_q', 'figure'),
              [Input('drop_year_q', 'value')])
def update_figure_q(selected_year_q):
    filtered_df_q = exposure_q[exposure_q["Year"] == selected_year_q]
    qtr = []
    for quarter in filtered_df_q["Quarters"].unique():
        df_by_quarter = filtered_df_q[filtered_df_q["Quarters"] == quarter]
        qtr.append(go.Bar(
            name='Exposure',
            x=df_by_quarter["Quarters"],
            y=df_by_quarter["Exposure"],
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
    filtered_df_m = exposure_q[exposure_m["Year"] == selected_year_m]
    mth = []
    for month in filtered_df_m["Months"].unique():
        df_by_month = filtered_df_m[filtered_df_m["Months"] == month]
        mth.append(go.Bar(
            name='Exposure',
            x=df_by_month["Months"],
            y=df_by_month["Exposure"],
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
    filtered_df_p_q = prod_q[prod_q["Year"] == selected_year_p_q]
    qtr_p = []
    for quarter in filtered_df_p_q["Quarters"].unique():
        df_p_by_quarter = filtered_df_p_q[filtered_df_p_q["Quarters"] == quarter]
        qtr_p.append(go.Bar(
            name='Production',
            x=df_p_by_quarter["Quarters"],
            y=df_p_by_quarter["Prod"],
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
    filtered_df_p_m = prod_m[prod_m["Year"] == selected_year_p_m]
    mth_p = []
    for month in filtered_df_p_m["Months"].unique():
        df_p_by_month = filtered_df_p_m[filtered_df_p_m["Months"] == month]
        mth_p.append(go.Bar(
            name='Production',
            x=df_p_by_month["Months"],
            y=df_p_by_month["Prod"],
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
    filtered_df_h_q = hedge_q[hedge_q["Year"] == selected_year_h_q]
    filtered_df_p_q_ = prod_q[prod_q["Year"] == selected_year_h_q]
    qtr_h_ppa = []
    qtr_h_oa = []
    qtr_h_cr = []
    for quarter in filtered_df_h_q["Quarters"].unique():
        df_h_by_quarter = filtered_df_h_q[filtered_df_h_q["Quarters"] == quarter]
        qtr_h_ppa.append(go.Bar(
            name='PPA',
            x=quarters,
            y=df_h_by_quarter.loc[df_h_by_quarter["TypeContract"]=='PPA', "Hedge"]),
            opacity=1,
            marker=dict(color=colors['ppa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ),
        qtr_h_oa.append(go.Bar(
            name='OA',
            x=quarters,
            y=df_h_by_quarter.loc[df_h_by_quarter["TypeContract"]=='OA', "Hedge"]),
            opacity=0.4,
            base=df_h_by_quarter.loc[df_h_by_quarter["TypeContract"]=='PPA', "Hedge"],
            marker=dict(color=colors['oa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ),
        qtr_h_cr.append(go.Bar(
            name='CR',
            x=quarters,
            y=df_h_by_quarter.loc[df_h_by_quarter["TypeContract"]=='CR', "Hedge"]),
            opacity=0.25,
            base=df_h_by_quarter.loc[df_h_by_quarter["TypeContract"]=='OA', "Hedge"],
            marker=dict(color=colors['cr']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ),

    for quarter in filtered_df_p_q_["Quarters"].unique():
        df_p_by_quarter_ = filtered_df_p_q_[filtered_df_p_q_["Quarters"] == quarter]
        qtr_p = []
        qtr_p.append(go.Bar(
            name='Production',
            x=quarters,
            y=df_p_by_quarter_["Prod"],
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
    filtered_df_h_m = hedge_m[hedge_m["Year"] == selected_year_h_m]
    filtered_df_p_m_ = prod_m[prod_m["Year"] == selected_year_h_m]
    mth_h_ppa = []
    mth_h_oa = []
    mth_h_cr = []
    for month in filtered_df_h_m["Months"].unique():
        df_h_by_month = filtered_df_h_m[filtered_df_h_m["Months"] == month]
        mth_h_ppa.append(go.Bar(
            x=months['months'],
            y=df_h_by_month.loc[df_h_by_month["TypeContract"]=='PPA', "Hedge"],
            opacity=1,
            marker=dict(color=colors['ppa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            )),
        mth_h_cr.append(go.Bar(
            name="CR",
            x=months['months'],
            y=df_h_by_month.loc[df_h_by_month["TypeContract"] == 'CR', "Hedge"],
            opacity=0.25,
            base=df_h_by_month.loc[df_h_by_month["TypeContract"] == 'OA', "Hedge"],
            marker=dict(color=colors['cr']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            )),
        mth_h_oa.append(go.Bar(
            name="0A",   
            x=months["Months"],
            y=df_h_by_month.loc[df_h_by_month["TypeContract"] == 'OA', "Hedge"],
            opacity=0.4,
            base=df_h_by_month.loc[df_h_by_month["TypeContract"] == 'PPA', "Hedge"],
            marker=dict(color=colors['oa']),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            )),

    for month in filtered_df_p_m_["Months"].unique():
        df_p_by_month_ = filtered_df_p_m_[filtered_df_p_m_["Months"] == month]
        mth_p = []
        mth_p.append(go.Bar(
            name='Production',
            x=months["Months"],
            y=df_p_by_month_["Prod"],
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
    filtered_df_m_ppa_m = h_ppa_m[h_ppa_m["Year"] == selected_year_m_ppa_m]
    filtered_df_not_oa_cr_m = prod_m_m[prod_m_m["Year"] == selected_year_m_ppa_m]
    mth_m_ppa = []
    mth_not_oa_cr = []
    for month in filtered_df_m_ppa_m["Months"].unique():
        df_m_ppa_by_mth = filtered_df_m_ppa_m[filtered_df_m_ppa_m["Months"] == month]
        mth_m_ppa.append(go.Bar(
            x=months,
            y=df_m_ppa_by_mth["PPA"],
            opacity=0.2,
            marker=dict(color=colors["PPA"]),
            marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
            ))
        mth_not_oa_cr.append(go.Bar(
             x=months,
             y=df_h_by_quarter["TypeContract"],
             opacity=1,
             marker=dict(color=colors["OA"]),
             marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']))),
    for month in filtered_df_not_oa_cr_m["Months"].unique():
        df_not_oa_cr_by_mth = filtered_df_not_oa_cr_m[filtered_df_not_oa_cr_m["Months"] == month]
        mth_not_oa_cr.append(go.Bar(
            x=months,
            y=df_not_oa_cr_by_mth["ProdMerchant"],
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
