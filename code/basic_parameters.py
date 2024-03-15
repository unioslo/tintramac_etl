#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

# Source data folder, i.e. location of excel files
# On knutwa's laptop
#excel_data_dir = '/Users/knutwa/OneDrive/tintramac_etl/Data entry template/'
excel_data_dir = '/Users/knutwa/OneDrive/Case Study Testing Data/'

output_dir = '/Users/knutwa/OneDrive/tintramac_etl/data/'

#database_name = "tintramac_utv"
database_name = "tintramac_test_data"


# Alternative paths
if not os.path.exists(excel_data_dir):
    excel_data_dir = 'Data entry template/'
