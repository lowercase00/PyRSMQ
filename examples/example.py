from pprint import pprint
import time

from rsmq.rsmq import RedisSMQ


# Create controller.
# In this case we are specifying the host and default queue name
# The controller uses the 0 database as default
queue = RedisSMQ(host="127.0.0.1", qname="myqueue")


# Start fresh
# Delete Queue if it already exists, ignoring exceptions
# Warning, can't be a problem for existing queues
queue.delete_queue().exceptions(False).execute()


# Create Queue with default visibility timeout of 20 and delay of 0
# demonstrating here both ways of setting parameters
# Both by passing kwargs or chaining methods
queue.create_queue(delay=0).vt(20).execute()


# Send a message with a 2 second delay
# See the queue status after the message is sent
message_id = queue.send_message(delay=2).message("Hello World").execute()
pprint({"queue_status": queue.get_queue_attr().execute()})


# Try to get a message
# this will not succeed, as our message has a delay and no other
# messages are in the queue
# Will print "False"
msg = queue.receive_message().exceptions(False).execute()
pprint({"Message": msg})


# Wait for our message to become visible
print("Waiting for our message to become visible")
time.sleep(2)


# Checkint the queue status
# Finally receive the message
pprint({"queue_status": queue.get_queue_attr().execute()})
msg = queue.receive_message().execute()
pprint({"Message": msg})


# Delete Message
# By id
queue.delete_message(id=msg["id"])
pprint({"queue_status": queue.get_queue_attr().execute()})

# delete our queue
# Clean up
queue.delete_queue().execute()

# No action
# Close the connection
queue.quit()
