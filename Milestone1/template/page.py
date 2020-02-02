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
        bytevalue = (value).to_bytes(8, byteorder='big')
        i = self.num_records * 8

        data[i + 0] = bytevalue[0]
        data[i + 1] = bytevalue[1]
        data[i + 2] = bytevalue[2]
        data[i + 3] = bytevalue[3]
        data[i + 4] = bytevalue[4]
        data[i + 5] = bytevalue[5]
        data[i + 6] = bytevalue[6]
        data[i + 7] = bytevalue[7]

    def read(self, index):
        bytevalue = bytearray(8)

        bytevalue[0] = data[index + 0]
        bytevalue[1] = data[index + 1]
        bytevalue[2] = data[index + 2]
        bytevalue[3] = data[index + 3]
        bytevalue[4] = data[index + 4]
        bytevalue[5] = data[index + 5]
        bytevalue[6] = data[index + 6]
        bytevalue[7] = data[index + 7]

        return int.from_bytes(bytevalue, byteorder='big')
