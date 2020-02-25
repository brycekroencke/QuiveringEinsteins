from table import Table
import os.path
from os import path
import json

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, file_name):

        file_name = file_name [2:] + ".json"
        self.file_name = file_name
        if (path.exists(self.file_name)):
            with open(self.file_name, "r") as read_file:
                data = json.load(read_file)
                for idi, i in enumerate(data):
                    print(i)#, len(i['book']))
                    #self.tables.append(Table(i, len(i['book'])-4, 0, file_name = file_name))



            #self.tables[0].construct_pd_and_index(self.tables[0].file_name)
            return

        else:
            print("Creating file for first time")
            with open(self.file_name, "w+") as write_file:
                return


    def close(self):
        #for pages in buffer pool, merge then and then write them to file
        # for i buffer_pool:
        #     self.tables[0].merge(i)
        #     self.tables[0].push_book_json(i)
        pass


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        ## CHECK IF TABLE EXISTS INSIDE THE DB FILE
        with open(self.file_name, "r") as read_file:
            try:
                data = json.load(read_file)
                if(data[str(name)]):
                    ## GET TABLE OBJECT AND RETURN IT
                    table = Table(name, num_columns, key, self.file_name) ## CHANGE TO BE THE PROPER TABLE FROM FILE
                else:
                    table = Table(name, num_columns, key, self.file_name)
            except ValueError:
                table = Table(name, num_columns, key, self.file_name)


            return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
