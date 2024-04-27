# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 17:06:34 2022

@author: hermann.ngayap
"""
import sys
import os
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from dash import dcc, html
from dashboards.scatter_plot.mtm_scatter_plot import MtM_sp
from dashboards.scatter_plot.mtm_merchant_scatter_plot import MtM_merchant_sp
from dashboards.scatter_plot.mtm_regulated_scatter_plot import MtM_regulated_sp
from dashboards.scatter_plot.mtm_historic_scatter_plot import MtM_H_sp

BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs


MtM_graph= html.Div(
    children=[
        html.Div(
            className="central-panel1",
            children=[
                html.Div(
                    children=[
# =============================================================================
#                         html.Div(
#                             style={
#                                 "display": "inline-block",
#                                 "vertical-align": "top",
#                                 "width": "30%",
#                             },
#                             children=[
#                                 html.Div(
#                                     children=[
#                                         MtM_sp
#                                         ],
#                                     className="custom-tab"
#                                     ),
#                                 ],
#                         ),
# =============================================================================
                        html.Div(
                            style={
                                "display": "inline-block",
                                "vertical-align": "top",
                                "width": "50%",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        MtM_merchant_sp
                                        ],
                                    className="custom-tab",
                                )
                            ],
                        ),
                        html.Div(
                            style={
                                "display": "inline-block",
                                "vertical-align": "top",
                                "width": "50%",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        MtM_regulated_sp
                                        ],
                                    className="custom-tab",
                                )
                            ],
                        )
                        
                    ]
                ),

            ],
        ),
    ],
    id="mtm_scatter_p_gr",
)