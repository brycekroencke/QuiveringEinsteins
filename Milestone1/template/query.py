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
        #putting metta data into a list and adding user
        #data to the list
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1
        mettaData = [0,self.table.ridcounter,0,0]
        mettaData_and_data = mettaData + data

        #ONLY EDIT BASE PAGES (base_list)
        #Check if self.table.base_list is empty -> add new book
        location = []
        if len(self.table.base_list) == 0:
            self.table.base_list.append(Book(len(columns), 0))
            location = self.table.base_list[-1].book_insert(mettaData_and_data)

        #Check if self.table.base_list newest book is full-> add new book
        elif self.table.base_list[-1].is_full():
            bookindex = self.table.base_list[-1].bookindex + 1
            self.table.base_list.append(Book(len(columns), bookindex))
            location = self.table.base_list[-1].book_insert(mettaData_and_data)

        #Check if self.table.base_list newest book has room -> add to end of book
        else:
            # Add data to end of newest book
            location = self.table.base_list[-1].book_insert(mettaData_and_data)

        #Setting RID key to book location value.
        self.table.page_directory[self.table.ridcounter] = location
        self.table.index.create_index(data[self.table.key], mettaData[1])


    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        RID_list = self.table.index.locate(key)
        records = []
        #Taking RIDS->location and extracting records into record list.
        for i in RID_list:
            #location[0] = book#, location[1] = row#
            location = self.table.page_directory[i]
            records.append(self.table.base_list[location[0]].record(location[1], self.table.key))

        return records


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
