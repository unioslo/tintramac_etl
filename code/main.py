#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process all the data
"""
from basic_parameters import excel_data_dir, output_dir, database_name

print('Read data from folder:', excel_data_dir)
print('Database name:', database_name)
print('Various output written to folder:', output_dir)

print("\n\n\n********Master data processing start*******\n")
import proc_master_data
print("\n********Master data processing end*******\n")

print("\n\n\n********Content data processing start*******\n")
import proc_content_data
print("\n********Content data processing end*******\n")

