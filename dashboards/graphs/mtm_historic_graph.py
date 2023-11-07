# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 22:55:22 2022

@author: hermann.ngayap
"""
from dash import dcc, html
from scatter_plot.MtM_H_scatter_plot import MtM_H_sp



MtM_H_graph= html.Div(
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
                                "width": "75%",
                            },
                            children=[
                                html.Div(
                                    children=[
                                        MtM_H_sp
                                        ],
                                    className="custom-tab"
                                    ),
                                ],
                        ),
                        
# =============================================================================
#                          html.Div(
#                              style={
#                                  "display": "inline-block",
#                                  "vertical-align": "top",
#                                  "width": "40%",
#                              },
#                              children=[
#                                  html.Div(
#                                      children=[
#                                          
#                                          ],
#                                      className="",
#                                  )
#                              ],
#                          )
# =============================================================================
                        
                    ]
                ),

            ],
        ),
    ],
    id="mtm_h_scatter_p_gr",
)
