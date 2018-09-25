"""
This is part 1 of 3 in a mini-project which deals with building a small ETL program in python. In this part, we
mainly deal with the E part of the ETL process, i.e. Extract. We extract and read in the data from two csv files
namely MLB2008.csv and StockValuation.csv. We provide the path to these files and use the concepts of polymorphism and
inheritance.
"""


import os
from csv import DictReader
# Get the file directory to specify it as the path for reading the csv file
file_directory = os.getcwd()


# abstract class
class AbstractRecord:
    # abstract class takes an instance variable name
    def __init__(self, name):
        self.name = name


class StockStatRecord(AbstractRecord):
    """StockStatRecord inherits AbstractRecord and has an initializer method that takes in data as arguments
     and returns a string of the data"""
    def __init__(self, ticker, company_name, exchange_country, price, exchange_rate, shares_outstanding, net_income, market_value_usd, pe_ratio):
        super().__init__(ticker)
        self.company_name = company_name
        self.exchange_country = exchange_country
        self.price = price
        self.exchange_rate = exchange_rate
        self.shares_outstanding = shares_outstanding
        self.net_income = net_income
        self.market_value_usd = market_value_usd
        self.pe_ratio = pe_ratio

    def __str__(self):
        # return data using string.format
        return ("StockStatRecord({0}, {1}, {2}, Price ${3:.2f}, Exchange Rate: ${4:.2f}, Shares Outstanding: {5:.2f}, Net Income: ${6}, Market Value: ${7:.2f}, PE Ratio: {8:.2f})".format(self.name,self.company_name,self.exchange_country,eval(self.price), eval(self.exchange_rate),
                    eval(self.shares_outstanding),eval(self.net_income),self.market_value_usd, self.pe_ratio))


class BaseballStatRecord(AbstractRecord):
    """BaseballStatRecord inherits AbstractRecord and has an initializer method that takes in data as arguments
         and returns a string of the data"""
    def __init__(self, player_name, salary, G, AVG):

        super().__init__(player_name)
        # self.player_name = player_name
        self.salary = salary
        self.G = G
        self.AVG = AVG

    def __str__(self):

        return "BaseballStatRecord({0}, Salary: ${1}, G: {2}, AVG: {3:.3f})".format(self.name, self.salary, self.G, eval(self.AVG))


class AbstractCSVReader:
    """This class is used to read the csv file and have an initializer method and row_to_record. The method load()
    returns the list of the records"""
    def __init__(self, file_path):

        self.file_path = file_path

    def row_to_record(self, row):
        # row is a row of the csv file
        self.row = row
        raise NotImplementedError

    def load(self):
        list_of_records = []
        # use with to open the csv file
        with open(self.file_path, 'r') as csv_file:
            current_data = DictReader(csv_file)
            for row in current_data:
                try:
                    record = self.row_to_record(row)
                    if record is not False and not None :
                        list_of_records.append(record)
                except BadData:
                    pass
            return list_of_records


class BaseballCSVReader(AbstractCSVReader):
    # Class to load the Baseball csv file
    def __init__(self, file_name):
        super().__init__(file_name)

    def row_to_record(self, row):
        # class implements its own row_to_record method
        # validation of the data
        try:
            # To check if the rows are not empty, if not return the data
            if row['PLAYER'] is not None and row['PLAYER'] != '' and row['SALARY'] is not None and row['SALARY'] != '' and row['G'] is not None and row['G'] != '' and row['AVG'] is not None and row['AVG'] != '':
                return BaseballStatRecord(row['PLAYER'],row['SALARY'],row['G'],row['AVG'])
            else:
                raise BadData()
        except ValueError as e:
            raise BadData()
        except Exception as e:
            raise BadData()


class StockCSVReader(AbstractCSVReader):
    # Class to load the Stocks valuation csv file
    def __init__(self, file_name):
        super().__init__(file_name)

    def row_to_record(self, row):
        # class implements its own row_to_record method
        # validation of the data
        try:
            # calculate the market_value_usd and pe_ratio values
            market_value_usd = eval(row['price']) * eval(row['exchange_rate']) * eval(row['shares_outstanding'])
            # To handle divide by zero
            if row['net_income'] == 0 or row['net_income'] == None:
                raise BadData()
            else:
                pe_ratio = eval(row['price']) / eval(row['net_income'])
             # Validation for empty rows and missing piece of information
            if row['ticker'] != '' and row['company_name'] != '' and row['exchange_country'] != '' and row['price'] != '' and row['exchange_rate']!= '' and row['shares_outstanding'] != '' and row['net_income'] != '':
                return StockStatRecord(row['ticker'], row['company_name'], row['exchange_country'], row['price'], row['exchange_rate'],
                         row['shares_outstanding'], row['net_income'], market_value_usd, pe_ratio)
            else:
                raise BadData()
        except ZeroDivisionError as e:
            return False
        except SyntaxError as e:
            return False
        except Exception as e:
            raise BadData()


class BadData(Exception):
    # Bad Data custom exception
    pass


if __name__ == "__main__":
    # From main load the file and print each record
    file_path_baseball = os.path.join(os.path.realpath(file_directory), 'MLB2008.csv')
    for baseballRecord in BaseballCSVReader(file_path_baseball).load():
        print(baseballRecord)

    print("\n" * 3)

    file_path_stocks = os.path.join(os.path.realpath(file_directory), 'StockValuations.csv')
    #records = StockCSVReader(file_path_stocks).load()
    for stockRecord in StockCSVReader(file_path_stocks).load():
        print(stockRecord)





