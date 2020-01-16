### --------------- Modules --------------- ###

import requests # Library to work with APIs
import pyodbc   # Connect to SQL server
import json     # Covert datastructures such as lists,tuples, objects etc to JSON strings
from datetime import datetime, date, timedelta # Work with date and times
import argparse # Parser for command-line
import sys # Use it with combination of argparse to parse user input 
import logging  # Provide logging capablities through static methods of logging class 


### --------------- Variables --------------- ###

today = datetime.now()
yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') # Get string representation of yesterday's date
day_before_yesterday_date = datetime.strftime(datetime.now() - timedelta(2), '%Y-%m-%d')

response_data_list=[]
payload_list=[]
date_list=[]
appnexus_exchange_rate_list=[]

ENDPOINT = "https://api.appnexus.com/currency"
new_currencies = ['ARS', 'AUD']

# table_name = '[currency].[dbo].[appnexus_exchanges]'
table_name = '[testdb].[dbo].[appnexus_exchanges]'
date_column = 'date'
currency_name_column = 'name'
sql_queries_dict = {
        'delete_all_rows_for_date_range' : '',
        'get_all_rows' : '',
        'insert_row' : '',
        'get_all_rows_for_date_range' : ''
        }


### --------------- Configuration --------------- ###

logging.basicConfig(filename='new_currencies_logging.log', level=logging.DEBUG, filemode='a', format='%(asctime)s - [%(levelname)s] :: %(message)s' )


### --------------- Models --------------- ###

class ExchangeRateRowModel: # Mapping of AppNexus.ExchangeRate table schema
    def __init__(self, exchange_date, name, rate_per_usd):
        self.exchange_date = exchange_date
        self.name = name
        self.rate_per_usd = rate_per_usd

class PayloadModel: # Mapping of AppNexus API payload params
   def __init__(self, code, show_rate, ymd):
        self.code = code
        self.show_rate = show_rate
        self.ymd = ymd


### --------------- Functions --------------- ###

# >> Database

def get_database_connector():
    logging.debug("\nYou are about to connect to database.\n")
    try:
        # sql_server_connector = pyodbc.connect(
        #     'Driver={ODBC Driver 17 for SQL Server};'
        #     'Server=TDGP220\PROD;'
        #     'Database=PA_DEV;'
        #     'Trusted_Connection=yes;'
        # )
        sql_server_connector = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=MAINDESKTOP\DB_PROD;'
            'Database=testdb;'
            'Trusted_Connection=yes;'
        )
        logging.debug("You are connected to PA_DEV")
        return sql_server_connector
    except:
        print('Failed to connect')
        logging.error('Failed connection to database')
        raise Exception('Failed connection to database')

def delete_rows_for_date_range(db_connector, cursor): # Delete rows for date input

    # print( '\nQuery: ' + sql_queries_dict['get_all_rows_for_date_range']+'\n')                #  DEBUGGING
    logging.debug('\nSQL query: ' + sql_queries_dict['get_all_rows_for_date_range'])

    cursor.execute(sql_queries_dict['get_all_rows_for_date_range'])

    logging.debug('Rows that will be deleted')

    count=0
    for row in cursor:
        logging.debug(row)
        #print(row)                                                                             #  DEBUGGING
        count = count + 1

    if count == 0:
        #print('\nEmpty result')                                                                #  DEBUGGING
        logging.debug('\nNo rows will be deleted.')
    else:
        #print('\nQuery: '+ sql_queries_dict['delete_all_rows_for_date_range']+'\n')            #  DEBUGGING
        print('\nStarting deleting rows')
        logging.debug('Starting deleting rows')

        cursor.execute(sql_queries_dict['delete_all_rows_for_date_range'])
        db_connector.commit()

        logging.debug('Done deleting rows')
        print('Done deleting rows\n')


def insert_rows_for_date_range(db_connector, cursor): # Insert rows for date input

    #print('Statement: ' + sql_queries_dict['insert_row'])                                      #  DEBUGGING
    print('\nStarting inserting rows')
    logging.debug('Starting inserting rows')
    for row in appnexus_exchange_rate_list:
        sql = sql_queries_dict['insert_row']
        cursor.execute(sql, (row.exchange_date, row.name, row.rate_per_usd))
        db_connector.commit()

    logging.debug('Done inserting rows')
    print('Done inserting rows\n')

# >> Population

def populate_sql_queries_dict(startdate, enddate): # Populate sql statements disctionary

        # Need to add dates in single quotes for correct queries
        q="'"
        startdate_in_single_quotes = "%s%s%s" % (q,startdate,q) 
        enddate_in_single_quotes = "%s%s%s" % (q,enddate,q) 

        # print(startdate_in_single_quotes + ' ' + enddate_in_single_quotes)                    #  DEBUGGING

        sql_queries_dict['delete_all_rows_for_date_range']='DELETE FROM %s WHERE %s BETWEEN %s AND %s' % (table_name, date_column, startdate_in_single_quotes, enddate_in_single_quotes)
        sql_queries_dict['get_all_rows']='SELECT * FROM %s' % table_name
        sql_queries_dict['insert_row']='INSERT INTO %s VALUES (?,?,?)' % table_name
        sql_queries_dict['get_all_rows_for_date_range']='SELECT * FROM %s WHERE %s BETWEEN %s AND %s' % (table_name, date_column, startdate_in_single_quotes, enddate_in_single_quotes)

def populate_payload_list(currency_name, date): # Creates payload object and append it to list
    payload = PayloadModel(currency_name, 'True', date)
    payload_list.append(payload)

def create_payload_for_custom_date(custom_date): # For each currency creates a PayloadModel object for the given date
    for currency in new_currencies: 
        populate_payload_list(currency, custom_date)

def populate_response_data_list(): # For each payload makes a GET request and append JSON data part to response_data_list
    for payload in payload_list:
        response_data = requests.get(ENDPOINT, params=vars(payload)).json() # vars function converts object to dict to get accepted as payload
        response_code = requests.get(ENDPOINT, params=vars(payload)).status_code

        if(response_code == 200):
            response_data_list.append(response_data)
        else:
            logging.critical('API issue for payload: ' 
            + payload.code + ' ' + payload.show_rate + '' + payload.ymd + ' at URL ' + ENDPOINT
            + '\n status code: ' + response_code)

def populare_appnexus_exchange_rate_list(): # For each response selects the desired properties and add them to a list of ExchangeRateRowModel
    for json_object in response_data_list:
        date = json_object['response']['currency']['as_of']
        name = json_object['response']['currency']['code']
        rate_per_usd  = json_object['response']['currency']['rate_per_usd']
        row = ExchangeRateRowModel(date, name, rate_per_usd)
        appnexus_exchange_rate_list.append(row)
        
def populate_date_list(arg_start_date, arg_end_date): # Creates a daterange list according touser's input 
    start_date = date(*map(int, arg_start_date.split('-'))) # converts string to date 'YYYY-MM-DD'
    end_date = date(*map(int, arg_end_date.split('-')))
    delta = timedelta(days=1)
    while start_date <= end_date:
        current_tmp_date = start_date.strftime("%Y-%m-%d")
        date_list.append(current_tmp_date)
        start_date += delta

def populate_exchange_rate_list_for_custom_date_range(): #  Generate ExchangeRateRowModel list for user input 
        #For each date generate a payload, so a payload list for each currency for each date is created.    
            # For each payload get the data part of the response
                # For each response get the desired fields by creating ExchangeRateRowModel object adding it to list 
    for date in date_list:
        create_payload_for_custom_date(date)
    log_payload_list()

    populate_response_data_list()
    log_response_data_list()

    populare_appnexus_exchange_rate_list()
    log_appnexus_exchange_rate_list()
    print_appnexus_exchange_rate_list()

    if(len(appnexus_exchange_rate_list)==len(date_list) * len(new_currencies)):
        logging.debug('Response objects match with excpected items')
        print('Response objects match with excpected items\n')
    else:
        raise Exception('Response objects do not match with excpected items\n' +
    'Response objects: {}, Expected objects: {}'.format(len(payload_list), len(date_list) * len(new_currencies)))
        print('Response objects do not match with excpected items')


# >> Printing - Logging

def pretty_print_json(response_data): # User friendly printing of JSON object
    pretty_text = json.dumps(response_data, sort_keys=True, indent=4) # Create formatted string of JSON object
    print(pretty_text)

def print_payload_list(): # Prints API payload list
    print('\nStart of Payload List: \n')
    for payload in payload_list:
        print(payload.code, payload.show_rate,payload.ymd)
    print('\nEnd of Payload List \n')

def print_response_data_list(): # Prints API response
    print('\nStart of Response Data List: \n')
    print('\nTotal objects: ', len(response_data_list))
    for response in response_data_list:
        pretty_print_json(response)
    print('\nEnd of Response Data List \n')

def print_appnexus_exchange_rate_list(): # Prints the object list that will be inserted to database 
    print('\nStart of AppNexus Exchange Rate List: \n')
    for row in appnexus_exchange_rate_list:
        print(row.exchange_date, row.name, row.rate_per_usd)
    print('\nEnd of AppNexus Exchange Rate List \n')
    

def pretty_log_json(response_data): # Logs JSON object is user friendly format
    pretty_text = json.dumps(response_data, sort_keys=True, indent=4) # Create formatted string of JSON object
    logging.debug(format(pretty_text))

def log_payload_list(): # Logs API payload list
    logging.debug('\nStart of Payload List: ')

    for payload in payload_list:
        log_output = payload.code + ' ' + payload.show_rate + ' ' + payload.ymd
        logging.debug(format(log_output))

    logging.debug('\nEnd of Payload List \n')

def log_response_data_list(): # Logs API response
    logging.debug('\nStart of Response Data List: \n')

    print('Total objects: ', len(response_data_list))

    log_output= len(response_data_list)
    logging.info('Total objects: {}'.format(log_output))

    for response in response_data_list:
        pretty_log_json(response)

    logging.debug('\nEnd of Response Data List \n')

def log_appnexus_exchange_rate_list(): # Logs the object list that will be inserted to database
    logging.debug('\nStart of AppNexus Exchange Rate List: \n')

    for row in appnexus_exchange_rate_list:
        log_output = row.exchange_date + ' ' + row.name + ' ' + row.rate_per_usd
        logging.debug(format(log_output))

    logging.debug('\nEnd of AppNexus Exchange Rate List \n')


# >> Validation

def validate_user_date_input(user_input): # Checks if user input is YYYY-MM-DD format
    try:
        datetime.strptime(user_input, '%Y-%m-%d') # convert string to datetime object

        logging.debug('The date {} is valid.'.format(user_input))
        print('The date {} is valid.'.format(user_input))
    except:
        logging.debug('The date {} is invalid'.format(user_input))
        raise Exception('The date {} is invalid'.format(user_input))


# >> Input Parsing

def parseArgument(): # Handles user arguments 

    parser = argparse.ArgumentParser(usage="This script retrive data from the AppNexus Exchange Rates and pushes them to SQL server")
    parser.add_argument('--startdate', required=False)
    parser.add_argument('--enddate', required=False)
    arguments = parser.parse_args()

    startdate_input = arguments.startdate
    enddate_input = arguments.enddate

    if startdate_input is None and enddate_input is None:

        enddate_input = yesterday_date
        startdate_input = enddate_input

        logging.debug('No arguments')
        print('No arguments')

    elif startdate_input == 'day_before_yesterday' and enddate_input is None:

        startdate_input = day_before_yesterday_date
        enddate_input = startdate_input

    elif startdate_input == 'yesterday' and enddate_input is None:

        startdate_input = yesterday_date
        enddate_input = startdate_input

    elif startdate_input and enddate_input in sys.argv:

        validate_user_date_input(startdate_input)
        validate_user_date_input(enddate_input)

        if(enddate_input < startdate_input):

            print('Input:\nStart date: ' + startdate_input + '\nEnd date:' + enddate_input + '\n')
            logging.debug('Input:\nStart date: ' + startdate_input + '\nEnd date:' + enddate_input 
            + '\nstart date cannot be bigger than End date')

            raise Exception('start date cannot be bigger than End date')

    elif enddate_input is None:

        validate_user_date_input(startdate_input)
        enddate_input=startdate_input

    elif startdate_input is None:

        logging.debug('startdate is None and enddate is not, this is not acceptable')
        raise Exception('startdate is None and enddate is not, this is not acceptable')

    logging.debug('\nInput:\nStart date: ' + startdate_input + '\nEnd date:' + enddate_input + '\n')    
    print('\nInput:\nStart date: ' + startdate_input + '\nEnd date:' + enddate_input + '\n')

    startdate_dict= startdate_input
    enddate_dict= enddate_input

    populate_sql_queries_dict(startdate_dict, enddate_dict)
    return startdate_input, enddate_input


### --------------- Script --------------- ###


def main():
    logging.debug('\n\n### --------------- Script started sos--------------- ###')

    startdate, enddate = parseArgument()

    db_connector=get_database_connector()
    cursor=db_connector.cursor()

    populate_date_list(startdate, enddate)
    populate_exchange_rate_list_for_custom_date_range()

    #delete_rows_for_date_range(db_connector, cursor)
    insert_rows_for_date_range(db_connector, cursor)

logging.debug('\n\n### --------------- Script ended eos--------------- ###')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Encoutered unhandled exception %s' % e)
        raise





    




