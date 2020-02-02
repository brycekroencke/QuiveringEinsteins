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
        self.space_remaining = 512
        self.content = [Page(), Page(), Page()]
        for i in range(num_of_pages):
            self.content.append(Page())
        print(len(self.content))

    def book_insert(self, *columns):
        self.space_remaining -= 1

        #columns was a tuple of tuples ((c1, c2, c3, c4, c5),)
        #convert columns to one list of ints and iterate through them
        columns = list(columns[0])
        for idx, i in enumerate(columns):
            continue
            #Convert each element i of to bytes and insert into byte array




            # bytes = i.to_bytes(length=8, byteorder='big')
            # for idx2, b in enumerate(bytes):
            #     #print(b.to_bytes(length=1, byteorder='big'))
            #     self.content[idx+3].data[idx2] = b

        #print(self.content[idx+3].data)
