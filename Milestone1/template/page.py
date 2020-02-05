from config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)


    # def has_capacity(self):
    #     return self.capacity

    def write(self, value):
        if self.has_space is False:
            print("ERROR: Page full.")
            return

        bytevalue = (value).to_bytes(8, byteorder='big')
        i = self.num_records * 8

        self.data[i + 0] = bytevalue[0]
        self.data[i + 1] = bytevalue[1]
        self.data[i + 2] = bytevalue[2]
        self.data[i + 3] = bytevalue[3]
        self.data[i + 4] = bytevalue[4]
        self.data[i + 5] = bytevalue[5]
        self.data[i + 6] = bytevalue[6]
        self.data[i + 7] = bytevalue[7]

        self.num_records += 1

    def update(self, value, index):
        if index > 512 - page.num_records:
            print("ERROR: Index too large.")
            return
        bytevalue = (value).to_bytes(8, byteorder='big')
        i = index * 8

        self.data[i + 0] = bytevalue[0]
        self.data[i + 1] = bytevalue[1]
        self.data[i + 2] = bytevalue[2]
        self.data[i + 3] = bytevalue[3]
        self.data[i + 4] = bytevalue[4]
        self.data[i + 5] = bytevalue[5]
        self.data[i + 6] = bytevalue[6]
        self.data[i + 7] = bytevalue[7]


    def read(self, index):
        #print("HERE: Index: " + str(index) + " self.num_records: " + str(self.num_records))
        if(index >= self.num_records):
            print("ERROR: Index out of range.")
            return

        rindex = index * 8
        bytevalue = bytearray(8)

        bytevalue[0] = self.data[rindex + 0]
        bytevalue[1] = self.data[rindex + 1]
        bytevalue[2] = self.data[rindex + 2]
        bytevalue[3] = self.data[rindex + 3]
        bytevalue[4] = self.data[rindex + 4]
        bytevalue[5] = self.data[rindex + 5]
        bytevalue[6] = self.data[rindex + 6]
        bytevalue[7] = self.data[rindex + 7]

        return int.from_bytes(bytevalue, byteorder='big')

    def delete(self, index):
        if(index >= self.num_records):
            print("ERROR: Index out of range.")
            return

        bytevalue = (0).to_bytes(8, byteorder='big')
        rindex = index * 8

        self.data[rindex + 0] = bytevalue[0]
        self.data[rindex + 1] = bytevalue[1]
        self.data[rindex + 2] = bytevalue[2]
        self.data[rindex + 3] = bytevalue[3]
        self.data[rindex + 4] = bytevalue[4]
        self.data[rindex + 5] = bytevalue[5]
        self.data[rindex + 6] = bytevalue[6]
        self.data[rindex + 7] = bytevalue[7]

    def has_space(self):
        if self.num_records < 512:
            return True
        else:
            return False
