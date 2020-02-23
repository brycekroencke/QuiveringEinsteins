class Buffer:
    def __init__(self):
        buffer_size = 2
        book_range = 1
        tail_list_length = int(buffer_size/book_range)
        
        self.base_book_list = [None]*buffer_size
        self.tail_book_list = [[]]*tail_list_length
