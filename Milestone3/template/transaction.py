from table import Table
from record import Record
from index import Index
from query import Query
import threading
from __init__ import *


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, table, id):
        self.table = table
        self.locks = []
        self.updates = []
        self.reads = {}
        self.queries = []
        self.transaction_id = id

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort

    # This MUST return 0 if transaction is sucessful, else it must return 0
    def run(self):
        print(self.transaction_id)
        for query, args in self.queries:
            key = args[0]
            exclusive = False
            if query.__name__ == "update":
                exclusive = True
            elif key in self.reads:
                result = self.reads[key]
                continue

            if self.secure_lock(key, exclusive) == False:
                print("Aborting transaciton #" + str(self.transaction_id))
                return self.abort()

            result = query(*args)

            if exclusive:
                self.updates.append(key)

                query = Query(self.table)
                self.reads[key] = query.select(key, self.table.key, [1]*self.table.num_columns)
            else:
                self.reads[key] = result

        return self.commit()

    def secure_lock(self, key, exclusive):
        locky = (key, exclusive)
        if self.locks.__contains__(locky) or self.locks.__contains__((key, True)):
            return True

        if self.table.acquire_lock(key, exclusive, self.transaction_id):
            self.locks.append(locky)
            return True

        return False

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        query = Query(self.table)

        for key in self.updates:
            query.change_link(key)

        self.release_locks()
        return False

    def commit(self):
        # TODO: commit to database
        self.release_locks()
        return True

    def release_locks(self):
        for locky in self.locks:
            self.table.release_lock(locky[0], self.transaction_id)
