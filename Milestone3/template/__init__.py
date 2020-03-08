import threading
threadLock = threading.Lock()

global_counter = 0

def inc_global_counter():
    with threadLock:
        global global_counter
        global_counter += 1
        return global_counter
