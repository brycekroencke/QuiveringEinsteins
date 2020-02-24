class Buffer:
    def __init__(self):
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
