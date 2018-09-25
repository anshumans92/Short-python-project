"""
This is the final part of the ETL project, and we will focus on the Transform part. We are creating a producer consumer
based Transform. We are modifying the Transform method we wrote in Part 1 by utilizing threads to make the code
run faster and print out the necessary information
"""

# Trying to use regular expression, import re for that
import re
import threading
import csv
import queue
from Sharma_Anshuman_Project_Part1 import *

# instance of a queue with a name of stocks_rows
stocks_rows = queue.Queue()
# instance of a queue with a name of stocks_records
stocks_records = queue.Queue()
"""
Since the validation for data was not defined as a method in Part 1, defined methods to get valid data in project part 3
"""


# Defining a method for checking float values
def checker(string):
    """
    To check for float digits, with two decimal. The first [0-9] will take in a digit, followed by a dot and
    other digits. The re.fullmatch functions returns a match object only if the regex matches the string entirely.
    """
    float = re.fullmatch(r'^[0-9]*(\.[0-9]+)|([0-9]+)$', string)
    if float is None:
        return False
    return True


# A method to validate the rows and only get valid data
def Valid(row):
    # Check if length is 7 and use the checker method defined above
    if len(row) == 7 and not str(row[1]).isnumeric() and not str(row[2]).isnumeric() \
            and checker(row[3]) and checker(row[4]) and checker(row[5]) and checker(row[6]):
        return True
    return False


# Define class Runnable
class Runnable:
    # __call__ method
    def __call__(self, *args, **kwargs):
        # loop forever (while True)
        while True:
            try:
                # get an element while setting timeout as 1
                row = stocks_rows.get(timeout=1)
                # Calling the valid row method defined above
                if Valid(row):
                    # Only if the row data is valid, use put to queue
                    stocks_records.put(row)
                # Skip invalid records
                else:
                    pass
            # Exception handling without printing a message for empty queue
            except queue.Empty:
                break
        # print using string.format using id
        print("{} working hard!!".format(id(self)))


# Define class FastStocksCSVReader
class FastStocksCSVReader:
    # define init method by taking the file path
    def __init__(self,file_path):

        self.filepath = file_path

    # Define the load method, use open to open the file
    def load(self):
        with open(self.filepath, 'r') as csvFile:
            dict = csv.reader(csvFile)
            for row in dict:
                if (len(row) > 0):
                    stocks_rows.put(row)
        # instance of a list named thread
        threads = []
        # Create 4 threads in a loop
        for iterator_for_thread in range(4):
            new_thread = threading.Thread(target=Runnable())
            # Start each thread
            new_thread.start()
            threads.append(new_thread)
        # Invoke join
        for thread in threads:
            thread.join()
        # Create a new list and take each record and append to the new list. Return the new list
        Stock_records_list = []
        while stocks_records.qsize() > 0:
            record = stocks_records.get()
            Stock_records_list.append(record)
        return Stock_records_list


if __name__ == '__main__':
    # Getting the path to the file and loading the records
    file_path_stocks = os.path.join(os.path.realpath(file_directory), 'StockValuations.csv')
    # Load the csv, print the records
    loaded_records = FastStocksCSVReader(file_path_stocks).load()
    for row in loaded_records:
        try:
            # Calculate PE. We use '%f' here to suppress scientific conversion
            PE = '%f' % (float(row[3])/float(row[6]))
            print("StockStatRecord({}, {}, $price={}, $Cap={}, P/E={})\n".format(row[0], row[2], row[3], row[5], PE))
        except ValueError as e:
            raise BadData

