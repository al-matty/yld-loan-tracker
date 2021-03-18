#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from functions import *

#    This script connects to the Ethereum network once a day (at 00:00 UTC),
#    and queries the yield.credit loan factory contract.
#    If there are any loans not yet in the database (csv file), it saves
#    them, so that each row in the database will represent one loan taken out. 
#    The script is supposed to be scheduled externally using crontab for example.






import os
from etherscan import Etherscan
from web3.auto.infura import w3

# Initialize Etherscan API
API_KEY = os.environ['ETHERSCAN_API_KEY']
etherscan = Etherscan(API_KEY)

# Connect to ETH Node (Infura)
eth = w3.eth

# Contract addresses
loan_address = '0xbFE28f2d7ade88008af64764eA16053F705CF1f0'
loan_fac = '0x49aF18b1ecA40Ef89cE7F605638cF675B70012A7'
token = '0xdcb01cc464238396e213a6fdd933e36796eaff9f'

# Get contract ABIs
ABI_LOAN_FAC = etherscan.get_contract_abi(address=loan_fac)
ABI_TOKEN = etherscan.get_contract_abi(address=token)
ABI_LOAN = etherscan.get_contract_abi(address=loan)






# Specify paths to where data files will be created / updated
datafile = 'yield_historic_loans.csv'
logfile = 'yield_logging.txt'


# Specify the preferred order how the variables should be stored in the csv file
# Don't change once first data has been written to datafile
var_order = [
    'loan_status', 'is_defaulted', 'address_borrower',  'principal',
    'collateral', 'interest', 'ts_start', 'ts_due', 'duration', 'ts_repaid',
    'collateral_balance', 'address_lender', 'liquidatable_t_allowance',
    'address_lending_token', 'address_collateral_token'
    ]


# Get list of all previously known loans (= loan_addresses in datafile)
data = pd.read_csv(datafile, index_col='loan_address')
loans_in_csv = set(data.index.tolist())


# Query smart contract to get addresses of all loans ever taken out
all_loans = get_all_loans(logfile=logfile)


# Create set of new loans taken out since last data query
not_yet_stored = all_loans - loans_in_csv


# Possibility: New loans found. Append them to csv.
if not_yet_stored:

    fresh_loans = {}
    
    # Append new loans to datafile
    for loan in not_yet_stored:
        
        fresh_loans[loan] = get_loan_data(loan)
        
    # Save to datafile (csv file)
    updateCSV(fresh_loans, fileName=datafile, order=var_order, verbose=False, logfile=logfile)










# TODO

def get_current_loans():
    pass

def get_loans_to_date():
    pass

def get_total_YLD_minted():
    pass

def get_YLD_supply():
    pass

def get_TVL():
    pass

def get_avg_loan_value():
    pass

def get_avg_duration():
    pass

def get_total_borrowed_value():
    pass

def get_borrowed_value(loan_address):
    '''Return value of loan for address'''
    pass


# Get an ordered list of tokens_borrowed (token types)
def tokens_borrowed
    pass



