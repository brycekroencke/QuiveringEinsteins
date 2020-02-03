from table import Table


class Index:

    def __init__(self):
        self.index = {}


    # returns the location of all records with the given value

    def locate(self, sid):
        if sid in self.index:
            return self.index[sid]
        else:
            print("The student ID doesn't exist.")

        # given a valid sid, return the rid so we
        # find the data associated with the sid.


    # optional: Create index on SID and RID.

    def create_index(self, sid, rid):
        if not sid in self.index:
            self.index[sid] = rid
        else:
            print("The student ID already exist.")

        # make a dictionary that maps sid as key to rid as value.
        # rid is passed as a tuple of book ID and row_index.


    # optional: Drop index of specific SID.

    def drop_index(self, sid):
        if sid in self.index:
            del self.index[sid]
        else:
            print("The student ID doesn't exist.")

        # del function deletes the key-value pair in a dictionary.
