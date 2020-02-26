import json
import os.path
from os import path
import sys

from page import *
from time import time
from index import *
from buffer import *
from book import Book

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_ID_COLUMN = 4


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, file_name = None):
        if (file_name):
            self.file_name = file_name
        else:
            print("No file name provided for table")
            self.file_name = ""
        self.name = name
        self.buffer_pool = Buffer()
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.ridcounter = 0
        self.tidcounter = (2**64) - 1
        self.index = [None] * num_columns
        self.index[key] = Index()
        self.last_written_book = [None, None, None] #[book index #, 0 book is not full or 1 for book is full, -1 book is on disk (any other number book is in buffer pool)]
        self.book_index = 0

    # def __merge(self):
    #     while True:
    #         merge_queue = buffer_pool.full_tail_book_list
    #         while len(merge_queue) != 0:
    #             curr_tail_book = merge_queue.



    def pull_book(self, bookindex):
        # Check if any empty slots
        slot = -1
        for idx, i in enumerate(self.buffer_pool.buffer):
            if i == None:
                slot = idx

        # if no empty slots
        if slot == -1:
            # replacement time
            slot = self.buffer_pool.find_LRU()

            # if the book is dirty
            if self.buffer_pool.dirty[slot]:
                # push that book first
                print("PUSHIN THE DIRTY BOOK FIRST")
                self.dump_book_json(self.buffer_pool.buffer[slot])

        # Now slot is ready to be pulled to
        self.buffer_pool.buffer[slot] = self.pull_book_json(bookindex)
        return slot

    def pull_base_and_tail(self, base_index):
        base_buff_indx = pull_book(base_index)
        #self.buffer_pool.buffer[slot].

    def pull_book_json(self, book_number):
        with open(self.file_name, "r") as read_file:
            data = json.load(read_file)
            data = data[self.name][str(book_number)]
            loaded_book = Book(len(data['page']), book_number)
            for idi, i in enumerate(data['page']):
                loaded_book.content[idi].data = eval(i)
            return loaded_book

    def book_in_bp(self, bookid):
        for idx, i in enumerate(self.buffer_pool.buffer):
            if (i.bookindex == bookid):
                return idx
        return -1

    def dump_book_json(self, actualBook):
        book_number = actualBook.bookindex
        if (path.exists(self.file_name)):
            with open(self.file_name, "r") as read_file:
                try: #file exists and is not empty
                    data = json.load(read_file)

                    book_data = {str(book_number): []}
                    page_data = {'page': []}
                    for idj, j in enumerate(actualBook.content):
                        page_data['page'].append( str(j.data))
                    data[self.name][str(book_number)] = page_data
                    with open(self.file_name, "w") as write_file:
                        json.dump(data, write_file, indent=2)

                except ValueError:
                    book_data = {str(book_number): []}
                    for idi, i in enumerate(self.buffer_pool.buffer):
                        data = {'page': []}
                        for idj, j in enumerate(i.content):
                            data['page'].append( str(j.data))
                        book_data[str(book_number)].append(data)
                    table_data = {self.name: book_data}
                    #print(table_data)
                    with open(self.file_name, "w") as write_file:
                        json.dump(table_data, write_file, indent=2)

        else:
            with open(self.file_name, "w+") as write_file:
                    book_data = {str(book_number): []}
                    for idi, i in enumerate(self.buffer_pool.base_book_list):
                        data = {'page': []}
                        for idj, j in enumerate(i.content):
                            data['page'].append( str(j.data))
                        book_data[str(book_number)].append(data)
                    table_data[self.name].append(book_data)
                    json.dump(table_data, write_file, indent=2)



    """
    reads all data in file and uses rid and key to reconstruct entire page page_directory
    and reconstructs primary index
    """
    def construct_pd_and_index(self):
        with open(self.file_name, "r") as read_file:
            data = json.load(read_file)
            data = data[self.name]

            for idx, x in enumerate(data):
                book_number = idx
                rid_page = Page()
                sid_page = Page()
                rid_page.data = eval(data[str(x)]['page'][1])
                sid_page.data = eval(data[str(x)]['page'][self.key + 4])
                for page_index in range(512):
                    rid = rid_page.read_no_index_check(page_index)
                    sid = sid_page.read_no_index_check(page_index)
                    if (rid != 0):
                        print("index: sid %d rid %d" % (sid, rid))
                        print(sid, rid, book_number, page_index)
                        self.page_directory[rid] = [book_number, page_index]
                        self.index[self.key].index[rid] = [sid]
        print(self.page_directory)
        print(self.index[self.key].index)

    # def pull_all_json(self):
    #     with open("data_file.json", "r") as read_file:
    #         data = json.load(read_file)
    #         list_of_books = []
    #         for idx, x in enumerate(data['book']):
    #             loaded_book = Book(len(x['page']), idx)
    #             for idi, i in enumerate(x['page']):
    #                 loaded_book.content[idi].data = eval(i)
    #             list_of_books.append(loaded_book)
    #
    #         self.base_list = list_of_books
    #         print(self.base_list[0].content)
    #         return list_of_books
