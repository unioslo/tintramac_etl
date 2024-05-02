#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

# Source data folder, i.e. location of Excel files
# On knutwa's laptop
excel_data_dir = '/Users/knutwa/OneDrive/tintramac_etl/Data entry template/'
#excel_data_dir = '/Users/knutwa/OneDrive/Case Study Testing Data/'

output_dir = '/Users/knutwa/OneDrive/tintramac_etl/data/utv'

database_name = "tintramac_utv"
#database_name = "tintramac_test_data"

# Set `database_user = None` to use default username
database_user = None

# Options to switch off constraints on database. Keep to True unless you know what you're doing.
strict_nulls = True
strict_dtypes = True
strict_pkeys = True
strict_fkeys = True


# Alternative paths
if not os.path.exists(excel_data_dir):
    excel_data_dir = 'Data entry template/'
