"""
Copyright (c) 2020 OptionMetrics, LLC
1776 Broadway, Suite 1800. New York, NY. 10019.
All rights reserved.
This software is the confidential and proprietary information of
OptionMetrics, LLC ("Confidential Information").

This script imports csv files into the designated table.
Extracts date of daily data from the file name, deletes data for that day
from the destination table before adding records from the csv.
Works for all Woodseer tables as long as the assumptions are met.

Assumptions:
    1. Table structure is an exact replica of the csv file data
    2. An additional column exists to track the date data was inserted into table
    3. Works for all Woodseer csv files that end in "YYYYMMDD19.csv"


usage: WoodseerDailyDataImporter.py [-h] --destinst Database_instance_name --destdb DB_NAME
                         --destuser DB_USER --destpass Password --desttbl Table in database
                         --sourcefilepath File path with name & ext
    , where:
        --importtype     - 'Full' or 'Daily' depending on the type of import
        --destinst       - The database server to store results to
        --destdb         - The database name to store results to
        --trustedconnection - Set to "yes" if using trusted connection (windows creds of logged account) to database
        --destuser       - The username to log into the database with. Not needed when "--trustedconnection=yes"
        --destpass       - The password to login with. Not needed when "--trustedconnection=yes"
        --desttbl        - The table where data will get transferred to
        --sourcefilepath - The full name of csv file with path
        --extracoldata   - Date to be used for inserted date column in YYYY-MM-DD format (used only for full refresh)


Example:
    python3 WoodseerDailyDataImporter.py --importtype=Daily --destinst=TSQL201 --destdb=IvyDBEurope
    --trustedconnection=yes --destuser=None --destpass=None --desttbl=Woodseer_securities_history
    --sourcefilepath=c:\\temp\\optionmetrics-europe-ftp_securities_2021070719.csv

    python3 WoodseerDailyDataImporter.py --importtype=Daily --destinst=TSQL201 --destdb=IvyDBEurope
    --trustedconnection=yes --destuser=None --destpass=None --desttbl=Woodseer_dividends_history
    --sourcefilepath=c:\\temp\\optionmetrics-europe-ftp_dividends_2021070719.csv

    python3 WoodseerDailyDataImporter.py --importtype=Daily --destinst=TSQL201 --destdb=IvyDBEurope
    --trustedconnection=yes --destuser=None --destpass=None --desttbl=Woodseer_disbursements_history
    --sourcefilepath=c:\\temp\\optionmetrics-europe-ftp_disbursements_2021070719.csv

    python3 WoodseerDailyDataImporter.py --importtype=full --destinst=TSQL201 --destdb=IvyDBEurope
    --trustedconnection=yes --destuser=None --destpass=None --desttbl=Woodseer_disbursements_history
    --sourcefilepath=c:\\temp\\optionmetrics-europe-ftp_disbursements.csv --extracoldata=2021-03-23

    python3 WoodseerDailyDataImporter.py --importtype=full --destinst=TSQL201 --destdb=IvyDBEurope
    --trustedconnection=yes --destuser=None --destpass=None --desttbl=Woodseer_dividends_history
    --sourcefilepath=c:\\temp\\optionmetrics-europe-ftp_dividends.csv --extracoldata=2021-03-23

Requirements:
   - python module 'pyodbc' (pip install pyodbc)

"""

import csv
import ast as parser
import pyodbc
import logging
import sys
import argparse
from datetime import datetime

logger = logging.getLogger()


def parse_args():
    """Parses argument parameters
    """

    args = argparse.ArgumentParser()
    args.add_argument('--importtype', action="store", dest='importtype',
                      type=str, help='The type of import we are performing: "daily" or "full".')
    args.add_argument('--destinst', action="store", dest='destinst',
                      type=str, help='The database server to store results to.')
    args.add_argument('--destdb', action='store', dest='destdb',
                      type=str, help='The database name to store results to')
    args.add_argument('--trustedconnection', action='store', dest='trustedconnection',
                      type=str, help='If we want windows auth to be used to connect to db specify '
                                     '"--trustedconnection=yes"')
    args.add_argument('--destuser', action="store", dest='destuser',
                      type=str, help='The username to log into the database with')
    args.add_argument('--destpass', action='store', dest='destpass',
                      type=str, help='The password to login with')
    args.add_argument('--desttbl', action='store', dest='desttbl',
                      type=str, help='Table where data will get transferred to')
    args.add_argument('--sourcefilepath', action='store', dest='sourcefilepath',
                      type=str, help='The full path for csv file along with extension')
    args.add_argument('--extracoldata', action='store', dest='extracoldata',
                      type=str,
                      help='Only use when doing a "full" import. Date will be used for inserted date column in '
                           '\'YYYY-MM-DD\' format. Not required for daily import, in this case the date will be '
                           'extracted from csv file.')
    return args.parse_args()


def initialize_logging(log, verbose):
    """Toggles verbose logging (INFO level) and initializes logging settings
    Args:
        log (logging.Logger): logger to initialize
        verbose (boolean): toggles verbose logging on if True or off if False
    """
    log.setLevel(logging.INFO if verbose else logging.WARN)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log.level)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s.%(msecs)d %(name)s %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S'))
    log.addHandler(handler)


def generate_insert_query(desttbl, columns):
    """Creates the insert query for data transfer
    """
    logger.info('Generating insert query for table ' + desttbl + '...')
    query = 'insert into dbo.' + desttbl + '({0}) values ({1})'
    query = query.format(','.join(columns), ','.join('?' * len(columns)))
    return query


def generate_delete_query(desttbl, extracol, extracoldata):
    """Creates the delete query for data transfer
       we delete data using the DiffDate column to ensure no
       dupes will get created in the destination table
    """
    logger.info('Generating delete query for table ' + desttbl + '...')
    delete_query = 'delete from dbo.' + desttbl + ' where ' + extracol + ' = ' + "'" + extracoldata + "'"
    return delete_query


def check_destination_table_existence(conn, destdb, desttbl):
    """Ensures the destination table exists
    """
    logger.info('Ensuring destination table exists: ' + desttbl + '...')
    try:
        check_query = 'Select top 1 * from ' + destdb + '.dbo.' + desttbl
        cursor = conn.cursor()
        cursor.execute(check_query)
        cursor.commit()
    except Exception as e:
        logger.error(e)
        logger.error('Could not find table in database')
        sys.exit(1)


def delete_table_data(conn, delete_query, extracoldata, desttbl):
    """Deletes data in the destination table if DateDiff column shows data exists
    """
    logger.info('Cleaning records for ' + extracoldata + ' in table ' + desttbl + '...')
    cursor = conn.cursor()
    cursor.execute(delete_query)
    cursor.commit()


def extract_date_from_filename(sourcefilepath=''):
    """For daily transfers we want to make sure we get the date from the csv file name
    """
    try:
        if len(sourcefilepath) > 14:
            date = sourcefilepath[-14:]
            date = date[:8]
            if date.isdigit():
                d = date
                extracoldata = '-'.join([d[:4], d[4:6], d[6:]])
                datetime.strptime(extracoldata, '%Y-%m-%d')
    except ValueError as err:
        logger.error(err)
        logger.error('Error parsing date. File name does not contain a valid date in YYYYMMDD format ' +
                     'or file name does not end with YYYYMMDD19.csv'
                     )
        sys.exit(1)
    return extracoldata


def establish_db_connection(destinst, trustedconnection, destuser, destpass, destdb, desttbl):
    """Create a connection string for connecting to the SQL server database
    """
    try:
        logging.info('Connecting to database for table ' + desttbl + '...')
        string = 'Driver={SQL Server};Server=' + destinst.strip() + ';' \
                 + 'Database=' + destdb.strip() + ';'
        if trustedconnection == 'yes':
            string = string + 'Trusted_Connection=yes;'
        else:
            string = string + 'User=' + destuser.strip() + ';Password=' + destpass.strip()+ ';Trusted_Connection=no;'
        pyodbc.connect(string)
    except Exception as e:
        logger.error(e)
        logger.error('A database connection could not be made, please check ' +
                     'the credentials provided and network connection'
                     )
        sys.exit(1)
    return pyodbc.connect(string)


def read_csv(sourcefilepath, desttbl):
    """Reads csv file for data import
    """
    logger.info('Reading csv file for table ' + desttbl + '...')
    with open(sourcefilepath, 'r') as csv_file:
        reader = csv.reader(csv_file)
        return list(reader)


def create_header_col_list(desttbl, header, extracol):
    """Gets column names for data transfer from csv header row.
       Also cleans up any special characters from column names
    """
    columns = []
    logger.info('Cleaning up column names for table ' + desttbl + '...')
    # Remove all spaces, "-" and "/" characters from header names in csv
    for value in header:
        value = (value.replace("//", ""))
        value = (value.replace("-", ""))
        value = (value.replace(' ', ''))
        columns.append(value)
    columns.append(extracol)
    return columns


def write_records_to_db(conn, readers, extracoldata, query, total):
    """Executes connection string to establish a database connection,
       writes records from csv file to SQL table
    """

    readers.pop(0)  # Don't want to read the headers as data...
    cursor = conn.cursor()
    counter = 1
    for reader in readers:
        if extracoldata != '':
            reader.append(extracoldata)
            cursor.execute(query, reader)

        if counter % 100 == 0:
            logging.info(str(counter) + ' of ' + str(total) + ' records copied...')
        counter = counter + 1
    cursor.commit()

    logging.info('Total of ' + str(counter) + ' records transferred from csv...')
    return counter


def copy_data_from_csv_to_sql_server_table(
        importtype,
        destinst,
        trustedconnection,
        destuser,
        destpass,
        destdb,
        desttbl,
        sourcefilepath,
        extracol,
        extracoldata
):
    """Wrapper function for importing data from csv file to sql table
     """

    failed = 1
    destuser=jabran
    destpass=pass
    conn = establish_db_connection(destinst, trustedconnection, destuser, destpass, destdb, desttbl)
    check_destination_table_existence(conn, destdb, desttbl)
    if importtype.lower() == 'daily':
        delete_query = generate_delete_query(desttbl, extracol, extracoldata)
        delete_table_data(conn, delete_query, extracoldata, desttbl)
    readers = read_csv(sourcefilepath, desttbl)
    total_recs_in_csv = len(readers)
    columns = create_header_col_list(desttbl, readers[0], extracol)
    insert_query = generate_insert_query(desttbl, columns)
    written = write_records_to_db(conn, readers, extracoldata, insert_query, total_recs_in_csv)
    logger.info('Data transfer completed for table ' + desttbl)
    if total_recs_in_csv == written:
        failed = 0
    return failed


def validate_inputs(importtype, destinst, destdb, trustedconnection, destuser, destpass, desttbl, sourcefilepath,
                    extracoldata):
    """Validates user input
     """
    logger.info('Validating input...')
    params = {
        'importtype': importtype
        , 'destinst': destinst
        , 'destdb': destdb
        , 'desttbl': desttbl
        , 'sourcefilepath': sourcefilepath
    }
    null_messages = {'importtype': "Please specify whether we are doing a full or daily import",
                     'destinst': "DB instance name must be not empty",
                     'destdb': "DB name must be not empty",
                     'desttbl': "Please provide a name for the destination table in database",
                     'sourcefilepath': "Please provide a valid path for the source csv file"
                     }
    # checks
    for param in params:
        message = null_messages[param]
        value = params[param]
        if value is None:
            print(message)
            sys.exit(1)

    if trustedconnection is not None and trustedconnection.lower() != 'yes':
        if destuser is None:
            print('Please provide a valid username for database connection or set --trustedconnection=yes')
            sys.exit(1)
        if destpass is None:
            print('Please provide a valid password for database connection or set --trustedconnection=yes')
            sys.exit(1)

    if extracoldata is None and importtype.lower() == 'full':
        print('You have specified FULL refresh, please provide what date to use for DiffDate column')
        sys.exit(1)

    if importtype.lower() == 'full' and extracoldata is not None:
        try:
            datetime.strptime(extracoldata, '%Y-%m-%d')
        except ValueError as err:
            logger.error(err)
            logger.error('Error parsing date in extracoldata param, either format is not \'YYYY-MM-DD\' ' +
                         'or the date contains an invalid year, month, or day')
            raise


def main():
    """Returns 1 for failure, 0 for success
     """

    initialize_logging(logger, 'verbose')

    # scripts execution starts here
    args = parse_args()

    validate_inputs(args.importtype, args.destinst, args.destdb, args.trustedconnection, args.destuser,
                    args.destpass, args.desttbl, args.sourcefilepath, args.extracoldata)

    if args.importtype.lower() == 'daily':
        date = extract_date_from_filename(args.sourcefilepath)
    else:
        date = args.extracoldata

    if date is not None:
        failed = copy_data_from_csv_to_sql_server_table(
            args.importtype.lower(),
            args.destinst.lower(),
            args.trustedconnection,
            args.destuser,
            args.destpass,
            args.destdb.lower(),
            args.desttbl.lower(),
            args.sourcefilepath.lower(),
            'DiffDateTime',
            date
        )

        if failed == 0:
            logger.info(args.sourcefilepath + " has been imported successfully.")
        else:
            logger.info(args.sourcefilepath + " failed to import.")
    return failed


if __name__ == "__main__":
    main()
