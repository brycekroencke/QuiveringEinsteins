from table import *
from index import Index
from book import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1
        mettaData = [0,self.table.ridcounter,0,0]
        mettaData_and_data = mettaData + data
        #ONLY EDIT BASE PAGES (base_list)
        #Check if self.table.base_list is empty -> add new book
        if len(self.table.base_list) == 0:
            self.table.base_list.append(Book(len(columns)+4, 0))
            self.table.base_list[-1].book_insert(mettaData_and_data)

        #Check if self.table.base_list newest book is full-> add new book
        elif self.table.base_list[-1].is_full():
            bookindex = self.table.base_list[-1].bookindex + 1
            self.table.base_list.append(Book(len(columns), bookindex))
            self.table.base_list[-1].book_insert(mettaData_and_data)

        #Check if self.table.base_list newest book has room -> add to end of book
        else:
            # Add data to end of newest book
            self.table.base_list[-1].book_insert(mettaData_and_data)


    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        return [Record(0, 0, [0,0,0,0,0])]

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        #ONLY EDIT TAIL PAGES (tail_list)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
