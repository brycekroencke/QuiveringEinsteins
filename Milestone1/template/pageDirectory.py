
#note I did not import any file in Page directory
#because I don't think it needs anything
class PageDirectory:

    def __init__(self):
        self.directory = {}

    #adds a value to the page directory by setting
    #the RID as a key and the book_num and row as values
    def addRecord(self,RID,book_num,row):
        if not RID in self.directory:
            location = [book_num,row]
            self.directory[RID] = location
        else:
            print("Page Directory already has this RID: " + str(RID) +" record has not been added!")

    def removeRecord(self,RID):
        if RID in self.directory:
            del self.directory[RID]
        else:
            print("Page Directory does not has this RID: " + str(RID) +" there was no record delete!")
