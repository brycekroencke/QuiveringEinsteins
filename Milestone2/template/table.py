import json
<<<<<<< Updated upstream
import os.path
from os import path
import sys

=======
import copy
import lock
>>>>>>> Stashed changes
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
        self.key = key
        self.buffer_pool = Buffer(self.key)
        self.num_columns = num_columns
        self.page_directory = {}
        self.ridcounter = 0
        self.tidcounter = (2**64) - 1
        self.index = [None] * num_columns
        self.index[key] = Index()
<<<<<<< Updated upstream
        self.last_written_book = [None, None, None] #[book index #, 0 book is not full or 1 for book is full, -1 book is on disk (any other number book is in buffer pool)]
        self.book_index = 0

    # def __merge(self):
    #     while True:
    #         merge_queue = buffer_pool.full_tail_book_list
    #         while len(merge_queue) != 0:
    #             curr_tail_book = merge_queue.

    def __del__(self):
        print ("%s: has been writen to file and deleted from buffer"%self.name)

    def push_book(self, ind):
        if self.buffer_pool.buffer[ind] != None and self.buffer_pool.buffer[ind].dirty_bit == True:
            self.dump_book_json(self.buffer_pool.buffer[ind])
=======
        self.merge_queue = []

    def __merge(self):
        while True:
             while len(self.merge_queue) != 0:
                 # Get the book to be merged from the merge queue.
                 curr_tailbook_index = self.merge_queue.pop(0)
                 # Traverse the buffer pool to find the same books
                 # with the same book index. If yes, pin that book.

                 # check if the tail book is in buffer pool.
                 for i in range(0, len(self.buffer_pool.buffer) - 1):
                     if self.buffer_pool.buffer[i].bookindex == curr_tailbook_index:
                          # pin the tail book first before looking base book
                         self.buffer_pool.buffer[i].increment_pin()
                         bid = self.buffer_pool.buffer[i].read(1,4)
                         basebook_index = self.page_directory[bid][0]
                         # check if base book exists in buffer right now
                         if check_basebook_in_buffer(basebook_index)[0]:
                            self.buffer_pool.buffer[check_basebook_in_buffer(basebook_index)[1]].increment_pin()
                            merge_base_and_tail(check_basebook_in_buffer(basebook_index)[1], i)
                            break
                         else:
                            basebook_buffer_position = pull_book(basebook_index)
                            # pin the base book
                            self.buffer_pool.buffer[basebook_buffer_position].increment_pin()
                            merge_base_and_tail(basebook_buffer_position, i)
                            break
                 continue

                 # The tail book not in buffer pool.
                 # pull the tail book from the disk
                 tailbook_buffer_position = pull_book(curr_tailbook_index)
                 self.buffer_pool.buffer[tailbook_buffer_position].increment_pin()
                 bid = self.buffer_pool.buffer[tailbook_buffer_position].read(1,4)
                 basebook_index = self.page_directory[bid][0]

                 if check_basebook_in_buffer(basebook_index)[0]:
                    self.buffer_pool.buffer[check_basebook_in_buffer(basebook_index)[1]].increment_pin()
                    merge_base_and_tail(check_basebook_in_buffer(basebook_index)[1],tailbook_buffer_position)
                 else:
                    basebook_buffer_position = pull_book(basebook_index)
                    self.buffer_pool.buffer[basebook_buffer_position].increment_pin()
                    merge_base_and_tail(basebook_buffer_position,tailbook_buffer_position)



    def check_basebook_in_buffer(self, basebook_index):
        for i in range(0, len(self.buffer_pool.buffer) - 1):
            if self.buffer_pool.buffer[i].bookindex == basebook_index:
                return [True, i]
        return [False]

    # base_bp = base book position in buffer pool.
    # Similary logic to tail_bp.
    def merge_base_and_tail(self, base_bp, tail_bp):
        # Copy the selected base book and set the book
        # index to be -1.
        copybook = copy.deepcopy(self.buffer_pool.buffer[base_bp])
        copybook.bookindex = -1
        # Set the TPS of the copy book.
        num_records = self.buffer_pool.buffer[tail_bp].page_num_record()
        last_rid_tailbook = self.buffer_pool.buffer[tail_bp].read(num_records, 1)
        copybook.tps = last_rid_tailbook

        # Update the records in the copy book.
        for k in copybook.page_num_record():
            tid = copybook.read(k, 0)
            # If tps > tid for the current base record,
            # the base record has been updated. Otherwise,
            # no need to update it.
            if tid < copybook.tps:
                # Get the single full record with only the user data.
                tail_record_index = self.page_directory[tid][1]
                tail_record = self.buffer_pool.buffer[tail_bp].get_full_record(tail_record_index)
                for m in range(5, len(copybook.content) - 1):
                    # tail_record[m] is a single tail record cell
                    # k is the current row of the copy book.
                    copybook.content[m].update(tail_record[m], k)


        # Swap the book index between two books.
        lock.acquire()
        temp = self.buffer_pool.buffer[base_bp].bookindex
        self.buffer_pool.buffer[base_bp].bookindex = copybook.bookindex
        copybook.bookindex = temp
        lock.release()

        # Overwrite the indirection column from old book
        # to the copy book in case an update happended
        # during the merge process.
        copybook.content[0] = self.buffer_pool.buffer[base_bp].content[0]

        # Swap the book data in the buffer pool.
        self.buffer_pool.buffer[base_bp] = copybook

        # Unpin the books.
        self.buffer_pool.buffer[base_bp].decrement_pin()
        self.buffer_pool.buffer[tail_bp].decrement_pin()



>>>>>>> Stashed changes

    def pull_book(self, bookindex):
        slot = self.make_room()
        self.buffer_pool.buffer[slot] = self.pull_book_json(bookindex)
        return slot

    #Makes room for a new book to be inserted into bp
    def make_room(self):
        # Check if any empty slots
        slot = -1
        for idx, i in enumerate(self.buffer_pool.buffer):
            if i == None:
                slot = idx
                self.buffer_pool.pin(slot)
                return slot

        # if no empty slots
        if slot == -1:
            # replacement time
            slot = self.buffer_pool.find_LRU()

            # if the book is dirty
            self.push_book(slot)

        # Now slot is ready to be pulled to
            self.buffer_pool.pin(slot)
        return slot

<<<<<<< Updated upstream
    def pull_base_and_tail(self, base_index):
        base_buff_indx = pull_book(base_index)

=======
>>>>>>> Stashed changes
    def pull_book_json(self, book_number):
        with open(self.file_name, "r") as read_file:
            data = json.load(read_file)
            data = data[self.name][str(book_number)]
            loaded_book = Book(len(data['page']) - 5, book_number)
            for idi, i in enumerate(data['page']):
                loaded_book.content[idi].data = eval(i)
            loaded_book.book_indirection_flag = data['i_flag']


            for i in range(512):
                if loaded_book.content[1].read_no_index_check(i) != 0:
                    for page in loaded_book.content:
                        page.num_records += 1

            return loaded_book

    def book_in_bp(self, bookid):
        for idx, i in enumerate(self.buffer_pool.buffer):
            if i == None:
                return -1
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
                    page_data = {'page': [], 'i_flag': actualBook.book_indirection_flag}
                    for idj, j in enumerate(actualBook.content):
                        page_data['page'].append( str(j.data))
                    data[self.name][str(book_number)] = page_data
                    with open(self.file_name, "w") as write_file:
                        json.dump(data, write_file, indent=2)

                except ValueError:
                    book_data = {str(book_number): []}
                    data = {self.name: {str(book_number) :{'page': [], 'i_flag': actualBook.book_indirection_flag}}}
                    for idj, j in enumerate(actualBook.content):
                        data[self.name][str(book_number)]['page'].append(str(j.data))
                    with open(self.file_name, "w") as write_file:
                         json.dump(data, write_file, indent=2)

        # else:
        #     with open(self.file_name, "w+") as write_file:
        #             book_data = {str(book_number): []}
        #             for idi, i in enumerate(self.buffer_pool.base_book_list):
        #                 data = {'page': [], 'i_flag': actualBook.book_indirection_flag}
        #                 for idj, j in enumerate(i.content):
        #                     data['page'].append( str(j.data))
        #                 book_data[str(book_number)].append(data)
        #             table_data[self.name].append(book_data)
        #             json.dump(table_data, write_file, indent=2)

    """
    set book uses book_in_bp and pull book an conbinds them together and returns
    the location of were the book is stored in the bp
    """
    def set_book(self,bookid):
        check = self.book_in_bp(bookid)
        if check != -1: #book is in dp just return its location in dp
            self.buffer_pool.pin(check)
            return check

        else: #book not in bp put it into bp
            return self.pull_book(bookid) #book is now in dp return its location in dp


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
                        self.page_directory[rid] = [book_number, page_index]
                        self.index[self.key].index[rid] = [sid]


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
                        self.page_directory[rid] = [book_number, page_index]
                        self.index[self.key].index[rid] = [sid]

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
