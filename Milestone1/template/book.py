from page import *

INDIRECTION_COLUMN = 0
DELETION_COLUMN = 1
#TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2

CURRENT_BOOK_ID = 0

class Book:
    def __init__(self, num_of_pages):
        self.id = CURRENT_BOOK_ID
        CURRENT_BOOK_ID += 1

        self.content = [Page(), Page(), Page()]
        for i in range(num_of_pages):
            self.content.append(Page())
        print(len(self.content))
    #def indirection(self):


Book(10)
