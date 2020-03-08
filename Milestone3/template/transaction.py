from table import Table
from record import Record
from index import Index
from __init__ import *


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.transaction_id = inc_global_counter()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

<<<<<<< HEAD

    # This MUST return 0 if transaction is sucessful, else it must return 0
=======
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
>>>>>>> 4637297d6d76b2dfec90dc73297d6ab98b0d9d69
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database
        return True
