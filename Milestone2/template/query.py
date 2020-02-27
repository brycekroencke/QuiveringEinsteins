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
        #putting metta data into a list and adding user
        #data to the list
        data = list(columns)
        self.table.ridcounter = self.table.ridcounter + 1
        mettaData = [0,self.table.ridcounter,0,0,self.table.ridcounter]
        mettaData_and_data = mettaData + data
        location = []
        lastw = self.table.last_written_book

        # if no books.
        if (lastw[0] == None):
            idx = self.table.buffer_pool.find_LRU() #gives me index of the slot in buffer_pool that was LRU
            self.table.buffer_pool.buffer[idx] = Book(len(columns), self.table.book_index)

            lastw = [self.table.book_index, 0, idx]
            self.table.book_index += 1
            self.table.buffer_pool.pin(idx)

        # book full
        elif (lastw[1] == 1):
            idx = self.table.buffer_pool.find_LRU() #gives me index of the slot in buffer_pool that was LRU

            # EITHER PUSH CURRENT BOOK TO DISK IN LRU HERE OR DISPOSE IF CLEAN.
            if self.table.buffer_pool.buffer[idx] != None and self.table.buffer_pool.dirty[idx] == True:
                self.table.dump_book_json(self.table.buffer_pool.buffer[idx])

            self.table.buffer_pool.buffer[idx] = Book(len(columns), self.table.book_index)
            print(self.table.book_index)

            lastw = [self.table.book_index, 0, idx]
            self.table.book_index += 1
            self.table.buffer_pool.pin(idx)

        # there is an available book.
        else:
            self.table.set_book(lastw[0])

        idx = lastw[2]
        location = self.table.buffer_pool.buffer[idx].book_insert(mettaData_and_data)

        if(self.table.buffer_pool.buffer[idx].is_full()):
            lastw[1] = 1

        self.table.buffer_pool.unpin(idx)
        self.table.last_written_book = lastw

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
            ind = self.table.set_book(location[0])
            booky = self.table.buffer_pool.buffer[ind]
            check_indirection = booky.get_indirection(location[1])

            if booky.read(location[1], 1) != 0: #checking to see if there is a delete
                if check_indirection == 0: #no indirection
                    records.append(booky.record(location[1], self.table.key))
                    self.table.buffer_pool.unpin(ind)
                else: #there is an indirection
                    self.table.buffer_pool.unpin(ind)
                    temp = self.table.page_directory[check_indirection]
                    tind = self.table.set_book(temp[0])
                    tbooky = self.table.buffer_pool.buffer[tind]

                    records.append(tbooky.record(temp[1], self.table.key))
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

        data = list(columns)

        self.table.tidcounter = self.table.tidcounter - 1

        pin_idx_list = [] #holds a list of idx that asosetate to  what has been pinned during update
        tail_location = [-1, -1] #for later use
        tail_book_R_bp = -1#for later use
        new_record =[]#for later use

        """
        step 1) were is the book located eather on disk or in buffer_pool? do a search
        """
        base_book_bp = self.table.set_book(location[0]) #now holds the location of where book is stored in bp
        check_indirection =  self.table.buffer_pool.buffer[base_book_bp].get_indirection(location[1])
        pin_idx_list.append(base_book_bp)

        if check_indirection == 0:
        #constructing the full new record
            new_record = self.table.buffer_pool.buffer[base_book_bp].get_full_record(location[1])
            for idx, i in enumerate(data):
                if i != None:
                    new_record[idx + 5] = i
            new_record[1] = self.table.tidcounter #note that the rid of the base record is already in the BASE_ID_COLUMN thanks to insert

        else: # there is indirection
            tail_location = self.table.page_directory[check_indirection] #[Book num, row num]
            tail_book_R_bp = self.table.set_book(tail_location[0])

            new_record = self.table.buffer_pool.buffer[tail_book_R_bp].get_full_record(tail_location[1])
            for idx, i in enumerate(data):
                if i != None:
                    new_record[idx + 5] = i
            new_record[1] = self.table.tidcounter #note that the rid of the base record is already in the BASE_ID_COLUMN thanks to insert
            pin_idx_list.append(tail_book_R_bp)

        """
        NOW New_record holds the value that i wish to append to a tail book
        """
        indir_flag = self.table.buffer_pool.buffer[base_book_bp].book_indirection_flag
        if indir_flag == -1: #need a new tail book
            new_slot = self.table.make_room()   #make room
            self.table.buffer_pool.buffer[new_slot] = Book(len(columns), self.table.book_index) #add book
            location = self.table.buffer_pool.buffer[new_slot].book_insert(new_record)#add record to book

            self.table.buffer_pool.buffer[base_book_bp].book_indirection_flag = self.table.book_index#set indirection flag in base book
            if self.table.buffer_pool.buffer[new_slot].bookindex == 9:
                print("==================================================")
                spaceleft= self.table.buffer_pool.buffer[new_slot].space_left()
                print(spaceleft)
                print(self.table.buffer_pool.buffer[new_slot].get_full_record(511-spaceleft))
                print("==================================================")
            self.table.book_index += 1
            pin_idx_list.append(new_slot)

        else: #there is an availbe book to write to
            if tail_location[0]== indir_flag: #the most recent record is stored on the most recent book
                location = self.table.buffer_pool.buffer[tail_book_R_bp].book_insert(new_record)
                if self.table.buffer_pool.buffer[tail_book_R_bp].bookindex == 9:
                    print("==================================================")
                    spaceleft= self.table.buffer_pool.buffer[tail_book_R_bp].space_left()
                    print(spaceleft)
                    print(self.table.buffer_pool.buffer[tail_book_R_bp].get_full_record(511-spaceleft))
                    print("==================================================")

                if self.table.buffer_pool.buffer[tail_book_R_bp].is_full(): # tail book is full set flag to -1
                    self.table.buffer_pool.buffer[base_book_bp].book_indirection_flag = -1
                    #DOOOOOO Merge
            else: #most recent record is on a differnt book than the most
                slot = self.table.set_book(indir_flag) #bring tail book onto the bp
                if self.table.buffer_pool.buffer[slot].is_full():
                    print("ERROR??????????????????????????????????????????????????????????????????????")
                    # for i in range(512):
                    #     print(self.table.buffer_pool.buffer[slot].get_full_record(i))
                    #     print(self.table.buffer_pool.buffer[slot].space_left())
                    #     print(self.table.buffer_pool.buffer[slot].bookindex)
                    # print("ERROR??????????????????????????????????????????????????????????????????????")
                if self.table.buffer_pool.buffer[slot].bookindex == 9 and self.table.buffer_pool.buffer[slot].is_full():
                    print("------------------------------------------")
                    print(new_record)
                    print("------------------------------------------")
                location = self.table.buffer_pool.buffer[slot].book_insert(new_record) #add record to book
                if self.table.buffer_pool.buffer[slot].bookindex == 9:
                    print("//////////////////////////////////////////////////")
                    spaceleft= self.table.buffer_pool.buffer[slot].space_left()
                    print(spaceleft)
                    print(self.table.buffer_pool.buffer[slot].get_full_record(511-spaceleft))
                    print("////////////////////////////////////////////////////")

                pin_idx_list.append(slot)
                if self.table.buffer_pool.buffer[slot].is_full(): # tail book is full set flag to -1
                    self.table.buffer_pool.buffer[base_book_bp].book_indirection_flag = -1
                    #DOOOOO MERGE


        self.table.page_directory[self.table.tidcounter] = location
        #update base_book indirection with new TID
        self.table.buffer_pool.buffer[base_book_bp].content[0].update(self.table.tidcounter, indirection_location[1])
        for i in pin_idx_list:
            self.table.buffer_pool.unpin(i)

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
