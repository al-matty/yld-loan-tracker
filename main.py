#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from etherscan import Etherscan
from web3.auto.infura import w3

# Initialize Etherscan API
API_KEY = os.environ['ETHERSCAN_API_KEY']
etherscan = Etherscan(API_KEY)

# Connect to ETH Node (Infura)
eth = w3.eth

# Contract addresses
loan = '0xbFE28f2d7ade88008af64764eA16053F705CF1f0'
loan_fac = '0x49aF18b1ecA40Ef89cE7F605638cF675B70012A7'
token = '0xdcb01cc464238396e213a6fdd933e36796eaff9f'

# Get contract ABIs
ABI_LOAN_FAC = etherscan.get_contract_abi(address=loan_fac)
ABI_TOKEN = etherscan.get_contract_abi(address=token)
ABI_LOAN = etherscan.get_contract_abi(address=loan)



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



