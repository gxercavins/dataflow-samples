import time
from random import choice, random

from google.cloud import pubsub_v1

project_id = "PROJECT_ID"
topic_name = "TOPIC_NAME"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

name_list = ['Paul', 'Michel', 'Hugo', 'Bob', 'Kevin', 'Bacon', 'Laura', 'Anne', 'John', 'Connor', 'Diane', 'Louis', 'Eva', 'Charles', 'Marta']

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())

while True:
    data = u'{}'.format(choice(name_list))
    # Data must be a bytestring
    data = data.encode('utf-8')
    # When you publish a message, the client returns a Future.
    message_future = publisher.publish(topic_path, data=data)
    message_future.add_done_callback(callback)
    time.sleep(random())

print('Published message IDs:')

# We must keep the main thread from exiting to allow it to process
# messages in the background.
while True:
    time.sleep(60)