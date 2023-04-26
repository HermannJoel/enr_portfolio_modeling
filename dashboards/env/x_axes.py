# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 11:52:56 2022

@author: hermann.ngayap
"""
import pandas as pd

list_year=[2022, 2023, 2024, 2025, 2026, 2027, 2028]
years= pd.DataFrame(list_year, columns=['years'])

list_quarter =['Q1', 'Q2', 'Q3', 'Q4']
quarters = pd.DataFrame(list_quarter, columns=['quarters'])

list_month = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
months = pd.DataFrame(list_month, columns=['months'])
