from page import *

class Book:
    def __init__(self, num_of_pages, bookindex):
        self.bookindex = bookindex
        self.content = [Page(), Page(), Page(), Page()]
        for i in range(num_of_pages):
            self.content.append(Page())

    def book_insert(self, *columns):
        columns = columns[0]
        if(len(columns) > len(self.content)):
            print("ERROR: Trying to insert too many columns")
            return

        for idx, i in enumerate(columns):
            self.content[idx].write(i)

        return [self.bookindex, self.content[0].num_records - 1]

    #returns value at page and index.
    def read(self, index, column):
        return self.content[column + 3].read(index)

    #returns true if book is full.
    def is_full(self):
        if self.space_left() == 0:
            return True
        else:
            return False

    #returns how many rows available in pages.
    def space_left(self):
        return 512 - self.content[3].num_records
