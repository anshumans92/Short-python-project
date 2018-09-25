"""This is the second file in Project part 2. This file imports data from Project part 1 and populates
that data in the database and table created in create_dbs file. We define two classes BaseballStatsDAO
and StockStatsDao with inser_records and select_all methods. We then load the files used in project part 1
and print out the relevant information"""


# Import the required modules and libraries
import collections
from Sharma_Anshuman_Project_Part1 import StockCSVReader
from Sharma_Anshuman_Project_Part1 import BaseballCSVReader
from collections import defaultdict
from Sharma_Anshuman_Create_dbs import *


# Define the AbstractDao class, with the required methods
class AbstractDAO():
    def __init__(self, db_name):
        self.db_name = db_name

    def insert_records(self, records):
        raise NotImplementedError  # To raise NotImplementedError

    def select_all(self):
        raise NotImplementedError

    def connect(self):
        db_connect = sqlite3.connect(self.db_name)
        return db_connect


class BaseballStatsDAO(AbstractDAO):
    # init taking no parameter and passing the db_name
    def __init__(self,db_name):
        super(BaseballStatsDAO, self).__init__(db_name)

    # define the insert_records method
    def insert_records(self, record):
        baseball_connection = super().connect()
        baseball_cursor = baseball_connection.cursor()
        # Getting the data and using Insert to put this read data into the created data table.
        for data in record:
            player = getattr(data, 'name')
            games = getattr(data, 'G')
            avg = getattr(data, 'AVG')
            salary = getattr(data, 'salary')
            # We use ? to avoid the risk of sql injection attack
            baseball_cursor.execute("INSERT INTO baseball_stat VALUES (?,?,?,?)", (player, games, avg, salary))
        baseball_connection.commit()
        baseball_connection.close()

    # define the select_all method, which will be used to query the database
    def select_all(self):
        # establish the connection
        baseball_connection = super().connect()
        baseball_cursor = baseball_connection.cursor()
        # create empty deque
        baseball_deque = collections.deque()
        # select command to get the stored data
        data = baseball_cursor.execute("SELECT player_name, games_played, average, salary FROM baseball_stat;")
        # for all records, append to the deque
        for result in data:
            BaseballStatRecord = result
            baseball_deque.append(BaseballStatRecord)
        baseball_connection.commit()
        baseball_connection.close()
        # return the deque
        return baseball_deque


class StockStatsDAO(AbstractDAO):
    # init taking no parameter and passing the db_name
    def __init__(self,db_name):
       super(StockStatsDAO, self).__init__(db_name)

    # define the insert_records method
    def insert_records(self, record):
        stock_connection = super().connect()
        stock_cursor = stock_connection.cursor()
        # Getting the data and using Insert to put this read data into the created data table
        for stockdata in record:
            ticker = getattr(stockdata, 'name')
            company_name = getattr(stockdata, 'company_name')
            country = getattr(stockdata, 'exchange_country')
            price = getattr(stockdata, 'price')
            exchange_rate = getattr(stockdata, 'exchange_rate')
            shares_outstanding = getattr(stockdata, 'shares_outstanding')
            net_income = getattr(stockdata, 'net_income')
            market_value = getattr(stockdata, 'market_value_usd')
            pe_ratio = getattr(stockdata, 'pe_ratio')
            # We use ? to avoid the risk of sql injection attack
            stock_cursor.execute("INSERT INTO stock_stats VALUES (?,?,?,?,?,?,?,?,?)", (
             company_name, ticker, country, price, exchange_rate, shares_outstanding, net_income, market_value,
            pe_ratio))
        stock_connection.commit()
        stock_connection.close()

    # define the select_all method, which will be used to query the database
    def select_all(self):
        # establish the connection
        stock_connection = super().connect()
        stock_cursor = stock_connection.cursor()
        # create empty deque
        stock_deque = collections.deque()
        # select command to get the stored data
        stock_data = stock_cursor.execute("SELECT company_name, ticker, exchange_country, price, exchange_rate, shares_outstanding, net_income, market_value_usd, pe_ratio FROM stock_stats;")
        # for all records, append to the deque
        for result in stock_data:
            StockStatRecord = result
            stock_deque.append(StockStatRecord)
        stock_connection.commit()
        stock_connection.close()
        return stock_deque


if __name__ == '__main__':

    # define a method to calculate the average
    def calculate_average(values):
        # initialize the sum to 0
        sum = 0
        # calculate the number of values
        number_of_values = len(values)
        # for all values calculate the new sum and return the average
        for iterator_for_average in values:
            sum += float(iterator_for_average)
        return float(round(sum / number_of_values, 2))


    # load MLB2008.csv using the BaseballCSVReader
    baseball_file = BaseballCSVReader('MLB2008.csv').load()

    # load MLB2008.csv using the BaseballCSVReader
    stock_records = StockCSVReader('StockValuations.csv').load()

    # Insert the loaded records into baseball database using Baseball DAO's insert_records.
    BaseballStatsDAO('baseball.db').insert_records(baseball_file)

    # Insert the loaded records into stocks database using Stocks DAO's insert_records
    StockStatsDAO('stocks.db').insert_records(stock_records)

    # Using the select_all method defined above, get the values
    baseball_file = BaseballStatsDAO('baseball.db').select_all()
    baseball_dict = defaultdict(list)

    # in the for loop using the dictionary get the data values
    for players in baseball_file:
         baseball_dict[players[2]].append(players[3])

    # for the key value pain in the dictionary, using the calculate average method, print the values in the required format
    for key, value in sorted(baseball_dict.items()):
        print('{0:.3f} ${1:,.2f}'.format(float(key), calculate_average(value)))

    print("\n" * 3)

    # Using the select_all method defined above, get the values
    stock_Dao = StockStatsDAO('stocks.db').select_all()
    # default dictionary with list
    stock_dict = defaultdict(list)
    # for values selected, using key and value to print out the values
    for stocks in stock_Dao:
      stock_dict[stocks[2]].append(stocks[0])
    for key, value in sorted(stock_dict.items()):
      print(str(key) + ' ' + str(len(value)))



