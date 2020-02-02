from config import *


class Page:

    def __init__(self):
        self.num_records = 0
        # self.capacity = 512
        self.data = bytearray(4096)


    # def has_capacity(self):
    #     return self.capacity



    def write(self, value):
        self.num_records += 1
        pass
