from table import Table, Record
from index import Index
from book import *


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
        #ONLY EDIT BASE PAGES (base_list)
        #Check if self.table.base_list is empty -> add new book
        if len(self.table.base_list) == 0:
            self.table.base_list.append(Book(len(columns)))
            self.table.base_list[-1].book_insert(columns)

        #Check if self.table.base_list newest book is full-> add new book
        elif self.table.base_list[-1].is_full():
            self.table.base_list.append(Book(len(columns)))
            self.table.base_list[-1].book_insert(columns)

        #Check if self.table.base_list newest book has room -> add to end of book
        else:
            # Add data to end of newest book
            self.table.base_list[-1].book_insert(columns)






    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

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
