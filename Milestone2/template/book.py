from page import *
from record import Record

class Book:

    def __init__(self, *param):
        if isinstance(param[0], int):
            self.bookindex = param[1]
            self.content = [Page(), Page(), Page(), Page(), Page()]
            self.book_indirection_flag = -1
            self.where_userData_starts = len(self.content)# so we can skip past the mettaData
            for i in range(param[0]):
                self.content.append(Page())
        else:
            self.bookindex = -1
            self.content = param

    def book_insert(self, *columns):
        columns = columns[0]
        if(len(columns) > len(self.content)):
            print("ERROR: Trying to insert too many columns")
            exit()

        for idx, i in enumerate(columns):
            self.content[idx].write(i)

        return [self.bookindex, self.content[0].num_records - 1]

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
        record = Record(self.read(index, 1), self.read(index, self.where_userData_starts + keyindex), [])
        columns = []
        for i in range(len(self.content)):
            if i < self.where_userData_starts:
                continue
            else:
                columns.append(self.read(index, i))

        record.columns = columns
        return record

    #returns true if book is full.
    def is_full(self):
        if self.content[1].num_records < 512:
            return False
        else:
            return True

    #returns how many rows available in pages.
    def space_left(self):
        return 512 - self.content[3].num_records
