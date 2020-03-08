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


    # This MUST return 0 if transaction is sucessful, else it must return 0
    def run(self):
        for query, args in self.queries:
            query(*args)
        return 1

    def abort(self):
        pass

    def commit(self):
        pass
