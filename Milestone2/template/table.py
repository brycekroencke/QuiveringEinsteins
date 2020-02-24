from page import *
from time import time
from index import *
from buffer import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.buffer_pool = Buffer()
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index()
        self.ridcounter = 0
        self.tidcounter = 0

    def __merge(self):
        while True:
            merge_queue = buffer_pool.full_tail_book_list
            while len(merge_queue) != 0:
                curr_tail_book = merge_queue.
