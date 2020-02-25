from table import *
from index import Index
from book import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_ID_COLUMN = 4

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        rid = self.table.index.locate(key)
        location = self.table.page_directory[rid[0]]

        book = self.table.base_list[location[0]]
        book.rid_to_zero(location[1])


    """
    # Insert a record with specified columns
    """

    def create_index(self, col):
        if (col >= self.table.num_columns):
            print("No can do, pal. Column outta range.")
        elif (self.table.index[col] != None):
            print("No can do, pal. Index already created.")
        else:
            self.table.index[col] = Index()
            #Now scan database here and fill in index

    def drop_index(self, col):
        if (col >= self.table.num_columns):
            print("No can do, pal. Column outta range.")
        elif (self.table.index[col] != None):
            self.table.index[col] = None

    def insert(self, *columns):
        print(self.table.ridcounter)
        #putting metta data into a list and adding user
        #data to the list
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1
        mettaData = [0,self.table.ridcounter,0,0]
        mettaData_and_data = mettaData + data
        location = [] #will hold [book index, row num]

        # if no books.
        if (self.table.last_written_book[0] == None):
            idx = self.table.buffer_pool.find_LRU() #gives me index of the slot in buffer_pool that was LRU
            self.table.buffer_pool.buffer[idx] = Book(len(columns), self.table.book_index)
            self.table.last_written_book = [self.table.book_index, 0, idx]
            self.table.book_index += 1
            self.table.buffer_pool.touched(idx)  #updating the LRU_tracker

        # book not in BP
        if (self.table.last_written_book[2] == -1):
            # pull in book to BP, set last_written_book data
            print("pullin in the stuff, baby")

        # book full
        if (self.table.last_written_book[1] == 1):
            idx = self.table.buffer_pool.find_LRU() #gives me index of the slot in buffer_pool that was LRU

            # EITHER PUSH CURRENT BOOK TO DISK IN LRU HERE OR DISPOSE IF CLEAN.
            if self.table.buffer_pool.buffer[idx] != None and self.table.buffer_pool.dirty[idx] == True:
                print("dumped book")
                # self.table.dump_book_json(self.table.buffer_pool.buffer[idx])

            self.table.buffer_pool.buffer[idx] = Book(len(columns), self.table.book_index)
            self.table.last_written_book = [self.table.book_index, 0, idx]
            self.table.book_index += 1
            self.table.buffer_pool.touched(idx)  #updating the LRU_tracker

        idx = self.table.last_written_book[2]
        print(idx)
        location = self.table.buffer_pool.buffer[idx].book_insert(mettaData_and_data)

        if(self.table.buffer_pool.buffer[idx].is_full()):
            self.table.last_written_book[1] = 1

        self.table.buffer_pool.touched(idx)  #updating the LRU_tracker

        #Setting RID key to book location value.
        self.table.page_directory[self.table.ridcounter] = location
        for i in range(len(self.table.index)):
            if(self.table.index[i] != None):
                self.table.index[i].add_to_index(data[i], mettaData[1])


    """
    # Read a record with specified key
    """

    def select(self, key, col, query_columns):
        records = []
        if(self.table.index[col] == None):
            # do scan
            print("mc-scan")
            return records

        RID_list = self.table.index[col].locate(key)
        #Taking RIDS->location and extracting records into record list.

        for i in RID_list:
            location = self.table.page_directory[i]
            ind = self.table.book_in_bp(location[0])
            if (ind == -1):
                # pull book into BP
                print("pullin in the stuff, baby")
                ind = self.table.book_in_bp(location[0])

            #check_indirection =  self.table.base_list[location[0]].get_indirection(location[1])
            self.table.buffer_pool.pin(ind)
            check_indirection = self.table.buffer_pool.buffer[location[0]].get_indirection(location[1])

            if self.table.buffer_pool.buffer[location[0]].read(location[1], 1) != 0: #checking to see if there is a delete
                if check_indirection == 0: #no indirection
                    records.append(self.table.buffer_pool.buffer[location[0]].record(location[1], self.table.key))
                    self.table.buffer_pool.unpin(ind)
                else: #there is an indirection
                    self.table.buffer_pool.unpin(ind)
                    temp = self.table.page_directory[check_indirection]

                    tind = self.table.book_in_bp(temp[0])
                    if (tind == -1):
                        # pull tail-book into BP
                        print("pullin in the stuff, baby")
                        tind = self.table.book_in_bp(temp[0])

                    self.table.buffer_pool.pin(tind)

                    records.append(self.table.buffer_pool.buffer[tind].record(temp[1], self.table.key))
                    self.table.buffer_pool.unpin(tind)

        for idx in enumerate(query_columns):
            if query_columns[idx[0]] == 0:
                for i in records:
                    i.columns[idx[0]] = None

        return records


    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        #columns will be stored in weird tuples need to fix
        #UPDATE needs to change read in books to handle inderection
        #ONLY EDIT TAIL PAGES (tail_list)
        #print(columns) #this gives me (none,#,none,none,none)
        RID = self.table.index[self.table.key].locate(key)
        location = self.table.page_directory[RID[0]] # returns [book num, row]
        indirection_location = location
        check_indirection =  self.table.buffer_pool.buffer[location[0]].get_indirection(location[1])
        data = list(columns)
        #self.table.ridcounter = self.table.ridcounter + 1
        self.table.tidcounter = self.table.tidcounter - 1
        tail_slot = int(location[0]/1)

        #if no inderection
        if  check_indirection == 0:
            base_data = self.table.buffer_pool.buffer[location[0]].get_full_record(location[1])

            #mettaData = [0,self.table.ridcounter,0,0]
            #mettaData_and_data = mettaData + data

            for idx, i in enumerate(data):
                if i != None:
                    base_data[idx + 5] = i

            base_data[1] = self.table.tidcounter


            if len(self.table.buffer_pool.tail_book_list[tail_slot]) == 0:
                self.table.buffer_pool.tail_book_list[tail_slot].append(Book(len(columns), 0))
                location = self.table.buffer_pool.tail_book_list[tail_slot][-1].book_insert(base_data)

                self.table.buffer_pool.tail_book_list[tail_slot].tailPage_counter += 1

            #Check if self.table.base_list newest book is full-> add new book
            elif self.table.buffer_pool.tail_book_list[tail_slot][-1].is_full():
                bookindex = self.table.buffer_pool.tail_book_list[tail_slot][-1].bookindex + 1
                self.table.buffer_pool.tail_book_list[tail_slot].append(Book(len(columns), bookindex))
                location = self.table.buffer_pool.tail_book_list[tail_slot][-1].book_insert(base_data)

                self.table.buffer_pool.tail_book_list[tail_slot].tailPage_counter += 1

            #Check if self.table.base_list newest book has room -> add to end of book
            else:
                # Add data to end of newest book
                location = self.table.buffer_pool.tail_book_list[tail_slot][-1].book_insert(base_data)

        #if there is an inderection
        else:
            location = self.table.page_directory[check_indirection]

            tail_data = self.table.buffer_pool.tail_book_list[tail_slot][location[0]].get_full_record(location[1])
            #mettaData = [0,self.table.ridcounter,0,0]
            #mettaData_and_data = mettaData + data

            for idx, i in enumerate(data):
                if i != None:
                    tail_data[idx + 5] = i

            tail_data[1] = self.table.tidcounter

            #if len(self.table.tail_list) == 0:
            #    self.table.tail_list.append(Book(len(columns), 0))
            #    location = self.table.tail_list[-1].book_insert(tail_data)

            #Check if self.table.base_list newest book is full-> add new book
            if self.table.buffer_pool.tail_book_list[tail_slot][-1].is_full():
                bookindex = self.table.buffer_pool.tail_book_list[tail_slot][-1].bookindex + 1
                self.table.buffer_pool.tail_book_list[tail_slot].append(Book(len(columns), bookindex))
                location = self.table.buffer_pool.tail_book_list[tail_slot][-1].book_insert(tail_data)

                self.table.buffer_pool.tail_book_list[tail_slot].tailPage_counter += 1

            #Check if self.table.base_list newest book has room -> add to end of book
            else:
                # Add data to end of newest book
                location = self.table.buffer_pool.tail_book_list[tail_slot][-1].book_insert(tail_data)


        self.table.page_directory[self.table.tidcounter] = location
        #update base_book inderection with new TID
        self.table.buffer_pool.buffer[indirection_location[0]].content[0].update(self.table.tidcounter, indirection_location[1])


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0

        # force start_range < end_range
        temp = start_range
        start_range = min(start_range, end_range)
        end_range = max(temp, end_range)

        current_key = start_range

        while current_key <= end_range:      # doing traversal from the start key_value to the end key_value
            if self.table.index[self.table.key].contains_key(current_key):
                query_column = []

                # initialize the column list with all 0 and mark the target column to 1
                for i in range(self.table.num_columns):
                    if i == aggregate_column_index:
                        query_column.append(1)
                    else:
                        query_column.append(0)

                # apply select function to find the corresponding value of given SID and column#, adding all found value to sum
                sum += self.select(current_key, 0, query_column)[0].columns[aggregate_column_index]

            current_key += 1

        return sum
