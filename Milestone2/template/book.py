from page import *
from record import Record

class Book:
    def __init__(self, *param):
        if(type(param[0] == int):
            default_construtor(*param)
        else:
            copy_constructor(*param)

    def default_construtor(self, num_of_pages, bookindex):
        self.bookindex = bookindex
        self.pin = 0
        self.dirty_bit = 0
        self.tps = 0
        self.tailPage_counter = 0
        self.content = [Page(), Page(), Page(), Page(), Page()]
        for i in range(num_of_pages):
            self.content.append(Page())

    def copy_constructor(self, old_book):
        self.bookindex = -1
        self.pin = old_book.pin
        self.dirty_bit = old_book.dirty_bit
        self.tps = old_book.tps
        self.tailPage_counter = old_book.tailPage_counter
        self.content = oldBook.content

    def book_insert(self, *columns):
        columns = columns[0]
        if(len(columns) > len(self.content)):
            print("ERROR: Trying to insert too many columns")
            exit()

        for idx, i in enumerate(columns):
            self.content[idx].write(i)

        return [self.bookindex, self.content[-1].num_records - 1]

    def increment_pin(self):
        self.pin = self.pin + 1

    def decrement_pin(self):
        self.pin = self.pin - 1

    def set_dirty_bit(self):
        self.dirty_bit = 1

    def remove_dirty_bit(self):
        self.dirty_bit = 0

    def rid_to_zero(self, index):
        self.content[1].delete(index)

    def get_indirection(self, index):
        return self.content[0].read(index)

    #returns value at page and index.
    def read(self, index, column):
        return self.content[column].read(index)

    def get_full_record(self, index):
        columns = []
        for i in range(len(self.content)):
            columns.append(self.read(index, i))
        return columns

    def record(self, index, keyindex): #returns latest record (even if in tail)
        record = Record(self.read(index, 1), self.read(index, 4 + keyindex), [])
        columns = []
        for i in range(len(self.content)):
            if i < 4:
                continue
            else:
                columns.append(self.read(index, i))

        record.columns = columns
        return record

    #returns true if book is full.
    def is_full(self):
        if self.space_left() == 0:
            return True
        else:
            return False

    #returns how many rows available in pages.
    def space_left(self):
        return 512 - self.content[3].num_records
