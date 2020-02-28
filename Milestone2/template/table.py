import json
import os.path
from os import path
import sys
import copy
from page import *
from time import time
from index import *
from buffer import *
from book import Book
import threading

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
        self.last_written_book = [None, None, None] #[book index #, 0 book is not full or 1 for book is full, -1 book is on disk (any other number book is in buffer pool)]
        self.book_index = 0
        self.merge_queue = []
        self.close = False
        self.merge_thread = threading.Thread(target=self.__merge,)
        self.lock = threading.Lock()

    def __del__(self):
        print ("%s: has been writen to file and deleted from buffer"%self.name)

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

    def push_book(self, ind):
        if self.buffer_pool.buffer[ind] != None and self.buffer_pool.buffer[ind].dirty_bit == True:
            self.dump_book_json(self.buffer_pool.buffer[ind])

    def check_basebook_in_buffer(self, basebook_index):
        for i in range(0, len(self.buffer_pool.buffer) - 1):
            if self.buffer_pool.buffer[i].bookindex == basebook_index:
                return [True, i]
        return [False, -1]


    def __merge(self):

        while True:
             if self.close == True:
                 return

             while len(self.merge_queue) != 0:
                 # Get the book to be merged from the merge queue.
                 curr_tailbook_index = self.merge_queue.pop(0)
                 # Traverse the buffer pool to find the same books
                 # with the same book index. If yes, pin that book.
                 tailind = self.set_book(curr_tailbook_index)
                 bid = self.buffer_pool.buffer[tailind].read(1,4)
                 baseindex = self.set_book(self.page_directory[bid][0])
                 self.merge_base_and_tail(tailind, baseindex)

    # base_bp = base book position in buffer pool.
    # Similary logic to tail_bp.
    def merge_base_and_tail(self, base_bp, tail_bp):
        # Copy the selected base book and set the book
        # index to be -1.
        copybook = copy.deepcopy(self.buffer_pool.buffer[base_bp])
        print( "merging: "+ str(copybook.bookindex))
        copybook.bookindex = -1
        # Set the TPS of the copy book.
        num_records = self.buffer_pool.buffer[tail_bp].page_num_record()
        #print("Page_num_record" + str(num_records))
        last_rid_tailbook = self.buffer_pool.buffer[tail_bp].read(num_records-1, 1)
        copybook.tps = last_rid_tailbook

        # Update the records in the copy book.
        for k in range(copybook.page_num_record()):
            tid = copybook.read(k, 0)
            # If tps > tid for the current base record,
            # the base record has been updated. Otherwise,
            # no need to update it.
            if tid < copybook.tps and tid != 0 :
                # Get the single full record with only the user data.
                tail_record_index = self.page_directory[tid][1]
                tail_record = self.buffer_pool.buffer[tail_bp].get_full_record(tail_record_index)
                for m in range(5, len(copybook.content) - 1):
                    # tail_record[m] is a single tail record cell
                    # k is the current row of the copy book.
                    copybook.content[m].update(tail_record[m], k)


        # Swap the book index between two books.
        self.lock.acquire()
        temp = self.buffer_pool.buffer[base_bp].bookindex
        self.buffer_pool.buffer[base_bp].bookindex = copybook.bookindex
        copybook.bookindex = temp
        self.lock.release()

        # Overwrite the indirection column from old book
        # to the copy book in case an update happended
        # during the merge process.
        copybook.content[0] = self.buffer_pool.buffer[base_bp].content[0]

        # Swap the book data in the buffer pool.
        self.buffer_pool.buffer[base_bp] = copybook

        # Unpin the books.
        #self.buffer_pool.buffer[base_bp].decrement_pin()
        #self.buffer_pool.buffer[tail_bp].decrement_pin()
        self.buffer_pool.unpin(base_bp)
        self.buffer_pool.unpin(tail_bp)

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
