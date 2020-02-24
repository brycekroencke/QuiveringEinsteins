class Buffer:
    def __init__(self):
<<<<<<< HEAD
        self.buffer_size = 2
        self.book_range = 1
        self.tail_list_length = int(self.buffer_size/self.book_range)

        self.LRU_tracker = [None]*self.buffer_size  #least resently used makes it so we can keep track of old non used books with time stamps

        for i in range(self.buffer_size):
            self.LRU_tracker[i] = self.buffer_size - i  #NOTE  how old it is position

        self.base_book_list = [None]*self.buffer_size
        self.tail_book_list = [[]]*self.tail_list_length

    # Return a list of tail book to be merged.
    # This list will be the merge_queue in the merge function
    # in Table class.
    def full_tail_book_list(self):
        merge_tail_books = []
        for tail_book in tail_book_list:
            # The tail_book_list store a list of pages.
            if len(tail_book) > 2:
                merge_tail_books.append(tail_book)
        return merge_tail_books

    def find_LRU(self):  #returns the postion of book that is the last least resently used
        for i in range(self.buffer_size):
            if self.LRU_tracker[i] == self.buffer_size:
                return i
    def touched(self,index):
        index_time = self.LRU_tracker[index]

        for i in range(self.buffer_size):

            if index_time == 1: #if we are looking at the most recently used don't do anything
                break

            elif i == index: #set the touched books time to 1 (the most resently touched)
                self.LRU_tracker[i] = 1

            elif self.LRU_tracker[i] > index_time: #time is greater than time of index passed don't increment
                pass

            else: #increment time
                self.LRU_tracker[i] = self.LRU_tracker[i] + 1


"""
# TESTING LRU_tracker
temp = Buffer()
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(0)
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(0)
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(0)
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(1)
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(1)
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(4)
print(temp.LRU_tracker)
print(temp.find_LRU())

temp.touched(2)
print(temp.LRU_tracker)
print(temp.find_LRU())
"""
=======
        buffer_size = 2
        book_range = 1
        tail_list_length = int(buffer_size/book_range)

        self.base_book_list = [None]*buffer_size
        self.tail_book_list = [[]]*tail_list_length

    # Return a list of tail book to be merged.
    # This list will be the merge_queue in the merge function
    # in Table class.
    def full_tail_book_list(self):
        merge_tail_books = []
        for tail_book in tail_book_list:
            # The tail_book_list store a list of pages.
            if len(tail_book) > 2:
                merge_tail_books.append(tail_book)
        return merge_tail_books
>>>>>>> dad54bbf5060b31d3dcc59f41bf27066749443a0
