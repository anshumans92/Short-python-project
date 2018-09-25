"""This is one of the two files which contribute to Project part2.
The code written below is for initialization and creation of the two databases for baseball and stocks
data and the creation of the tables. This file will be use to create the database into which the
data read in Project part 1 will be stored."""

import sqlite3

sqlite_file1 = 'baseball.db'    # name of the baseball sqlite database file
sqlite_file2 = 'stocks.db'     # name of the stocks sqlite database file
table_name1 = 'baseball_stat'
table_name2 = 'stock_stats'
column1 = 'player_name'
column1_type = 'text'
column2 = 'games_played'
column2_type = 'int'
column3 = 'average'
column3_type = 'real'
column4 = 'salary'
column4_type = 'real'
column5 = 'company_name'
column5_type = 'text'
column6 = 'ticker'
column6_type = 'text'
column7 = 'exchange_country'
column7_type = 'text'
column8 = 'price'
column8_type = 'real'
column9 = 'exchange_rate'
column9_type = 'real'
column10 = 'shares_outstanding'
column10_type = 'real'
column11 = 'net_income'
column11_type = 'real'
column12 = 'market_value_usd'
column12_type = 'real'
column13 = 'pe_ratio'
column13_type = 'real'

# Connecting to the first database using with statement
with sqlite3.connect(sqlite_file1) as conn:
    c = conn.cursor()
    try:
        # Creating the sqlite table with columns defined above
        c.execute('''CREATE TABLE IF NOT EXISTS {0} ( 
                  {1}      {2},     
                  {3}      {4}, 
                  {5}      {6}, 
                  {7}      {8}
                  );'''.format(table_name1,column1, column1_type, column2, column2_type, column3, column3_type, column4, column4_type))
    except Exception as e:    # In case of exceptions
        conn.rollback()
        raise e
# To commit the changes/transactions made to the database
conn.commit()

# Connecting to the first database using with statement
with sqlite3.connect(sqlite_file2) as conn1:

    c1 = conn1.cursor()
    try:
        c1.execute('''CREATE TABLE IF NOT EXISTS {0} (  
                        {1}      {2},     
                        {3}      {4}, 
                        {5}      {6}, 
                        {7}      {8},
                        {9}      {10},
                        {11}     {12},
                        {13}     {14},
                        {15}     {16},
                        {17}     {18}
                        );'''.format(table_name2, column5, column5_type, column6, column6_type, column7, column7_type, column8,
                                     column8_type, column9, column9_type, column10, column10_type, column11, column11_type, column12, column12_type, column13, column13_type))
    except Exception as e:
        conn1.rollback()
        raise e

conn1.commit()
