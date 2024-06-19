#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

# Source data folder, i.e. location of Excel files
# excel_data_dir = '/Users/knutwa/OneDrive/tintramac_etl/Data entry template/'
excel_data_dir = '/Users/knutwa/OneDrive/Case Study Testing Data/'

# csv ouput etc goes here:
output_dir = '/Users/knutwa/OneDrive/tintramac_etl/data/case_study'

# database_name = "tintramac_utv"
database_name = "tintramac_test_data"

# Set `database_user = None` to use default username
database_user = None

# Options to switch off constraints on database. Keep True unless you know what you're doing.
strict_pkeys = True # Set primary keys
strict_fkeys = True # Set foreign keys. Only runs strict_pkeys is True
strict_nulls = True # Enforce non-null property where applicable
strict_dtypes = True # Enforces data types

# You can declare alternative paths like this:
# if not os.path.exists(excel_data_dir):
#    excel_data_dir = 'Data entry template/'

# Connect to an external database by replacing hostname none with the host URL
hostname = None
# hostname = "dbpg-uio-prod02.uio.no"