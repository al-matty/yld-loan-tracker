#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
#    This script connects to the Ethereum network and queries the
#    yield.credit loan factory contract. It conditionally creates or appends
#    platform metrics to csv files. Any file not existing yet will be created.
#    The script is intended to be scheduled daily using crontab for example.
#############################################################################

print('Script started...')

import os
from lookups import type_map, token_map
from functions import log, get_abi, get_token_price


# Specify paths to data files. Files will be created if not found.
csv_active_loans = 'yield_active_loans.csv'     # replaced daily
csv_hist_loans = 'yield_hist_loan_activity.csv' # appended to if a loan status changes
csv_daily_metrics = 'yield_daily_metrics.csv'   # appended to daily
logfile = 'yield_logging.txt'                   # appended to daily


# Specify the preferred order how the variables (columns )should be stored
# in the csv file. Don't change once first data has been written to file.

var_order = [
    'loan_status', 'is_defaulted', 'address_borrower',  'principal',
    'collateral', 'interest', 'ts_start', 'ts_due', 'duration', 'ts_repaid',
    'collateral_balance', 'address_lender', 'liquidatable_t_allowance',
    'address_lending_token', 'address_collateral_token'
    ]


#############################################################################
#
# Set global variables BTC_PRICE, ALL_LOANS, LOAN_FAC, ALL_LOANS_DATA
#
#############################################################################


# BTC_PRICE <- current BTC price used for BTC-denominated metrics
try:
    BTC_PRICE = get_token_price('bitcoin')
except:
    message = "Couldn't scrape BTC price from Coingecko."
    print(message)
    log(logfile, message)

print(f'Successfully scraped current BTC price: {round(BTC_PRICE)} USD')
print('Updating database now...')


# Contract addresses
loan_address = '0xbFE28f2d7ade88008af64764eA16053F705CF1f0'
loan_fac_address = '0x49aF18b1ecA40Ef89cE7F605638cF675B70012A7'
yld_token_address = '0xdcb01cc464238396e213a6fdd933e36796eaff9f'
# Get contract ABIs

ABI_LOAN_FAC = get_abi(loan_fac_address)
#abi_token = get_abi(yld_token_address)
ABI_LOAN = get_abi(loan_address)

from functions import *

print('Successfully connected to APIs...')





#############################################################################
#
# Update database with current loan information
#
#############################################################################


# Get not yet repaid loans. Save to csv.
print(f'Saving currently active loans to \'{csv_active_loans}\'...')
replace_active_loans(csv_active_loans, csv_hist_loans, var_order, logfile)

# Get loans that got defaulted/repaid since last update. Append to csv.
print('Checking for loans with a recently changed status...')
update_hist_loans(csv_hist_loans, var_order, logfile=logfile)



# TODO: Get metrics for frontend

# Scrapes data for tokens used in loans so far & saves to csv
#update_daily_metrics(csv_daily_metrics)

# Reads from csv
#metrics_dict = get_metrics_for_frontend()
# updates global TOKEN_METRICS_TODAY
