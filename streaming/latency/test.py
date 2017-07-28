import datetime
import time
import sys

def utf8len(s):
    return len(s.encode('utf-8'))


#msg_s = int(sys.argv[1])

msg_s = 5000


sleep_time = 1
extra_size = []
for i in xrange(msg_s):
    extra_size.append(msg_s)
    
extra_message_str = str(extra_size)

print utf8len(extra_message_str)




