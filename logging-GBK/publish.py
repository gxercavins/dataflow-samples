#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This application demonstrates how to perform basic operations on topics
with the Cloud Pub/Sub API.

For more information, see the README.md under /pubsub and the documentation
at https://cloud.google.com/pubsub/docs.
"""

import argparse
import time

from google.cloud import pubsub_v1


def publish_messages(project, topic_name):
    """Publishes multiple messages to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    # publish one message with key 'Random'
    data = u'Random test'
    data = data.encode('utf-8')
    publisher.publish(topic_path, data=data)

    # publish two messages with key 'Quick'
    data = u'Quick test'
    data = data.encode('utf-8')
    publisher.publish(topic_path, data=data)
    publisher.publish(topic_path, data=data)
 
    # sleep to wait for next window 
    print('Sleeping for a minute.')
    time.sleep(60)

    # publish one more message with key 'Quick'
    publisher.publish(topic_path, data=data)

    print('Published messages.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('project', help='Your Google Cloud project ID')

    subparsers = parser.add_subparsers(dest='command')

    publish_parser = subparsers.add_parser(
        'publish', help=publish_messages.__doc__)
    publish_parser.add_argument('topic_name')

    args = parser.parse_args()

    if args.command == 'publish':
        print('Publishing messages... Press CTRL+C to interrupt.')
        publish_messages(args.project, args.topic_name)
