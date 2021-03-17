# Helper Functions

import os, inspect, sys
import random
import pandas as pd
from time import sleep

# Helper function needed by get_loan_data
def extract_loan_details(loan_dict):
    '''Splits up 'loan_details' into single entries, deletes original.'''

    details = loan_dict['loan_details']
    
    # Add keys and values to dict
    loan_dict['address_lender'] = details[0]
    loan_dict['address_borrower'] = details[1]
    loan_dict['address_lending_token'] = details[2]
    loan_dict['address_collateral_token'] = details[3]
    loan_dict['principal'] = details[4]
    loan_dict['interest'] = details[5]
    loan_dict['duration'] = details[6]
    loan_dict['collateral'] = details[7]
    
    # Remove original entry
    del loan_dict['loan_details']
    return loan_dict


# Helper function needed by get_loan_data
def extract_meta_data(loan_dict):
    '''Splits up 'meta_data' into single entries, deletes original.'''

    meta = loan_dict['meta_data']
    
    # Add keys and values to dict
    loan_dict['loan_status'] = meta[0]
    loan_dict['ts_start'] = meta[1]
    loan_dict['ts_repaid'] = meta[2]
    loan_dict['liquidatable_t_allowance'] = meta[3]
    
    # Remove original entry
    del loan_dict['meta_data']
    return loan_dict


# Get a dict of loan data for a loan_address
def get_loan_data(loan_address):
    '''Returns a dictionary of loan data.'''
    d = {}
    # Instantiate contract to make it callable
    loan = eth.contract(address=loan_address, abi=ABI_LOAN)
    caller = loan.caller()
    d['collateral_balance'] = caller.getCollateralBalance()
    d['loan_details'] = caller.getLoanDetails()
    d['meta_data'] = caller.getLoanMetadata()
    d['ts_due'] = caller.getTimestampDue()
    d['is_defaulted'] = caller.isDefaulted()
        
    d = extract_loan_details(d)
    d = extract_meta_data(d)

    return d


# Appends a new row to csv as specified in fileName
def appendToCsv(fileName, varList, varNames, verbose=True):
    '''
    Appends each value in varList as a new row to a file as specified in fileName.
    Creates new file with header if not found in working dir.
    Aborts with error message if it would change shape[1] of csv (= number of vars per row).
    
    Format of header:    id,time,[varNames]
    Example for row:     0,2021 Feb 18 16:24,0.03,72,NaN,Yes,...
    
    1st value: Successive id (=first value in last row of file + 1).
    2nd value: The current time in format "2021 Feb 18 17:34"
    If there is no file yet: Creates file with header = id, timestamp, [varNames]
    '''
    
    # Get name of function for error messages (depends on inspect, sys)
    funcName = inspect.currentframe().f_code.co_name
    
    # Abort if number of variables and names don't add up.
    assert len(varList) == len(varNames), \
        f"{funcName}(): The number of variables and names to append to csv must be the same."
    
    rowsAdded = []
    
    # Get current time.
    timestamp = datetime.now()
    parsedTime = timestamp.strftime('%Y %b %d %H:%M')

    # Possibility: fileName doesn't exist yet. Create file with header and data.
    if not os.path.isfile(fileName):
        header = 'id,' + 'time,' + str(','.join(varNames))
        
        with open(fileName, 'a') as wfile:
            wfile.write(header)
            varList = [str(var) for var in varList]
            row = '\n' + '0' + ',' + parsedTime + ',' + str(','.join(varList))
            wfile.write(row)
            rowsAdded.append(row)
            
        if verbose:
            print(
            '''
            No file called "%s" has been found, so it has been created.
            Header:
            %s
            ''' % (fileName, header))
            print('Added new row to data: \t', row[1:]) 

    # Possibility: fileName exists. Only append new data.
    else:
        
        # Abort if number of variables to append differs from number of elements in csv header.
        with open(fileName, 'r') as infile:
            header = infile.readlines()[0]
            n_header = len(header.split(','))
            
        assert len(varList) + 2 == n_header, \
            f"""
            {funcName}(): You're trying to append a row of {len(varList)} variables to csv.
            In the csv header there are {n_header}. To be imported as pandas dataframe for analytics,
            the number of variables per row in the csv needs to stay consistent throughout all rows.
            """
        
        # Determine new id value based on most recent line of file.
        with open(fileName, 'r') as rfile:
            rows = rfile.readlines()
            try:
                # Write id, time, data to file.
                id_ = str(int(rows[-1].split(',')[0]) + 1)
                with open(fileName, 'a') as wfile:
                    varList = [str(var) for var in varList]
                    row = '\n' + str(id_) + ',' + parsedTime + ',' + str(','.join(varList))
                    wfile.write(row)
                    rowsAdded.append(row)
                    
                    if verbose:
                        print('Added new row to data: \t', row[1:]) 

            # Possibility: id can't be determined from file. Abort.
            except ValueError:
                print('''
                The last line of "%s" doesn't start with a valid id value (int).
                Something is wrong with your data file.
                No data has been written to the file.''' % fileName)

                
# Calls appendToCsv(). Values in d (nested dict) per pool/token become veriables per row in csv
def updateCSV(d, fileName, order=None, verbose=True, logfile=None):
    '''
    Appends current pool data from nested dict to csv file to keep track of
    asset ratios over time.
    Order can be specified as list of variable names.
    logfile: If a textfile is specified, appends datetime & #rows to logfile.
    '''
    outDf = pd.DataFrame(d)

    # Possibility: Reorder data as specified in order
    if order:
        outDf = outDf.reindex(order)
        
    # Count rows before appending data
    if os.path.isfile(fileName):
        with open(fileName, 'r') as file:
            rowsBefore = len(file.readlines())
    else:
        rowsBefore = 0
    
    # Append data
    for pair in outDf:
        name = pair
        varNames = outDf[pair].index.tolist()
        varNames.insert(0, 'token')
        varList = outDf[pair].values.tolist()
        varList.insert(0, name)
        appendToCsv(fileName, varList, varNames, verbose=verbose)
        
    # Count rows after appending, prepare labeled sample row for printing
    with open(fileName, 'r') as file:
        lines = file.readlines()
        rowsAfter = len(lines)
        difference = rowsAfter - rowsBefore
        headerList = lines[0].strip().split(',')
        sampleRow = random.choice(lines[-difference:])
        sampleList = sampleRow.strip().split(',')
        
    printDf = pd.DataFrame(sampleList, index=headerList, columns=['Sample Row'])
    
    print(f'Appended {difference} rows to {fileName}.\nRandom sample:\n')
    print(printDf)
    
    # Option: Write summary to logfile
    if logfile:
        log(logfile, f'Appended {difference} rows to {fileName}.')

        
# Appends a row (datetime + log message) to a logfile.
def log(logfile, _str):
    '''
    Appends current date, time, and _str as row to logfile (i.e. logging.txt).
    '''
    # Get current time.
    timestamp = datetime.now()
    parsedTime = timestamp.strftime('%Y %b %d %H:%M')
    
    row = '\n' + parsedTime + '\t' + _str
    
    with open(logfile, 'a') as file:
        file.write(row)