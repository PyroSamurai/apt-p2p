quick example:

import khashmir, threading
k = khashmir.Khashmir('127.0.0.1', 4444)
start_new_thread(k.dispatcher.run, ())
k.addContact('127.0.0.1', 8080)  # right now we don't do gethostbyname
k.populateTable()


alternatively, you can call k.dispatcher.runOnce() periodically from whatever thread you choose