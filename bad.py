import csv
import pyodbc

table_name = 'Woodseer_securities_history'
path = 'c:\\temp\\optionmetrics-europe-ftp_securities.csv'

# table_name = 'Woodseer_dividends_history'
# path = 'c:\\temp\\optionmetrics-europe-ftp_dividends.csv'

#table_name = 'Woodseer_disbursements_history'
#path = 'c:\\temp\\optionmetrics-europe-ftp_disbursements.csv'

conn = pyodbc.connect(
                      'Driver={SQL Server};'
                      'Server=TSQL201;'
                      'Database=IvyDBEurope;'
                      'Trusted_Connection=no;'
                      'User=Jabran;'
                      'pass=Password;'
                      )

with open(path, 'r') as csv_file:
    reader = csv.reader(csv_file)
    header = next(reader)
    columns = []

    # Remove all spaces, "-" and "/" characters from header names in csv
    for value in header:
        value = (value.replace("//", ""))
        value = (value.replace("-", ""))
        value = (value.replace(' ', ''))
        columns.append(value)
    columns.append('DiffDateTime')

    query = 'insert into dbo.' + table_name + '({0}) values ({1})'
    query = query.format(','.join(columns), ','.join('?' * len(columns)))

    cursor = conn.cursor()
    for data in reader:
        date = data.append('2021-03-23')
        print (data)
        cursor.execute(query, data)
    cursor.commit()
