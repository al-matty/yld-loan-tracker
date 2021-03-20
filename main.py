#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
#    This script connects to the Ethereum network and queries the
#    yield.credit loan factory contract. It conditionally creates or appends
#    platform metrics to csv files. Any file not existing yet will be created.
#    The script is intended to be scheduled daily using crontab for example.
#############################################################################

import os
#from functions import *
#import update_hist
#from lookups import type_map, token_map
from etherscan import Etherscan
from web3.auto.infura import w3


print('Script started...')


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


# Initialize Etherscan API
API_KEY = os.environ['ETHERSCAN_API_KEY']
etherscan = Etherscan(API_KEY)

# Connect to ETH Node (Infura)
eth = w3.eth

# Contract addresses
loan_address = '0xbFE28f2d7ade88008af64764eA16053F705CF1f0'
loan_fac_address = '0x49aF18b1ecA40Ef89cE7F605638cF675B70012A7'
yld_token_address = '0xdcb01cc464238396e213a6fdd933e36796eaff9f'

# Get contract ABIs
abi_loan_fac = etherscan.get_contract_abi(address=loan_fac_address)
abi_token = etherscan.get_contract_abi(address=yld_token_address)
abi_loan = etherscan.get_contract_abi(address=loan_address)

# Set some global variables
LOAN_FAC = eth.contract(address=loan_fac_address, abi=abi_loan_fac).caller
ALL_LOANS = get_all_loans(logfile=logfile)    # all loan addresses ever created
ALL_LOANS_DATA = {loan: get_loan_data(loan) for loan in ALL_LOANS}
DAILY_METRICS = {}
#YLD_TOKEN = eth.contract(address=yld_token_address, abi=abi_token)#.caller
try:
    BTC_PRICE = get_token_price('bitcoin')
except:
    message = 'Couldn\'t scrape BTC price from Coingecko.'
    print(message)
    log(logfile, message)

print('Successfully connected to APIs...')