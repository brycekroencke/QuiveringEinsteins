import json

from page import *
from time import time
from index import *
from buffer import *
from book import Book

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
print((2**64)-1)

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
        self.ridcounter = 0
        self.tidcounter = (2**64) - 1
        self.index = [None] * num_columns
        self.index[key] = Index()

    def __merge(self):
        while True:
            merge_queue = buffer_pool.full_tail_book_list
            while len(merge_queue) != 0:
                curr_tail_book = merge_queue.


    #MOSTLY FOR DEBUGGING IN BEGINNING
    def pull_book_json(self, book_number):
        with open("data_file.json", "r") as read_file:
            data = json.load(read_file)
            book_to_load = data['book'][book_number]
            loaded_book = Book(len(book_to_load['page']), book_number)
            for idi, i in enumerate(book_to_load['page']):
                loaded_book.content[idi].data = eval(i)
            return loaded_book

    # #MOSTLY FOR DEBUGGING IN BEGINNING
    # def dump_all_json(self):
    #     with open("data_file.json", "w+") as write_file:
    #         book_data = {'book': []}
    #         for idi, i in enumerate(self.base_list):
    #             data = {'page': []}
    #             for idj, j in enumerate(i.content):
    #                 data['page'].append( str(j.data))
    #             book_data['book'].append(data)
    #         json.dump(book_data, write_file, indent=2)


    def dump_book_json(self, book_number):
        with open("data_file.json", "r") as read_file:
            data = json.load(read_file)
            if(book_number in range(0, len(data['book']))):
                data['book'][book_number]['page'] = []
                for idj, j in enumerate(self.base_list[book_number].content):
                    data['book'][book_number]['page'].append( str(j.data))
                with open("data_file.json", "w") as write_file:
                    json.dump(data, write_file, indent=2)
            else:
                print("does not exist")
                page_data = {'page': []}
                for idj, j in enumerate(self.base_list[book_number].content):
                    page_data['page'].append( str(j.data))
                data['book'].append(page_data)
                with open("data_file.json", "w") as write_file:
                    json.dump(data, write_file, indent=2)


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
