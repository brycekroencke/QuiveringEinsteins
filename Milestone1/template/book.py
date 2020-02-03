from page import *

INDIRECTION_COLUMN = 0
DELETION_COLUMN = 1
#TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2

#CURRENT_BOOK_ID = 0

class Book:
    def __init__(self, num_of_pages):
        self.id = 0 #CURRENT_BOOK_ID
        #CURRENT_BOOK_ID += 1
        self.content = [Page(), Page(), Page()]
        for i in range(num_of_pages):
            self.content.append(Page())

    def book_insert(self, *columns):
        if(len(columns) > len(self.content) - 3):
            print("ERROR: Trying to insert too many columns")
            return

        columns = list(columns[0])
        for idx, i in enumerate(columns):
            self.content[idx + 3].write(i)

    #returns value at page and index.
    def read(self, column, index):
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
