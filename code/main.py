#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 13:42:58 2024

@author: knutwa
"""
from database_tools import close_db
from basic_parameters import excel_data_dir, output_dir, database_name

import proc_metadata
import proc_content_data

print('Read data from folder:', excel_data_dir)
print('Database name:', database_name)
print('Various output to folder:', output_dir)
# close_db()