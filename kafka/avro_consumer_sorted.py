#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This is a simple example of the SerializingProducer using Avro.
#

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from datetime import datetime, timedelta
import toml
import argparse
from collections import deque
import matplotlib.pyplot as plt


class Event(object):
    """
    An object representing a sensor event

    Args:
        id (str): Sensor's id

        timestamp (datetime): timestamp when the event happened

        value (double): Sensor's reading value

    """
    def __init__(self, id, timestamp, value):
        self.id = id
        self.timestamp = timestamp
        self.value = value


def dict_to_event(obj, ctx):
    """
    Converts object literal(dict) to an Event instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    """
    if obj is None:
        return None

    return Event(id=obj['id'],
                 timestamp=obj['timestamp'],
                 value=obj['value'])


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Consumes events from kafka topic hosted at a HopsWorks cluster.')
    parser.add_argument("-c", "--config", default='config.toml',
                        help='Configuration file in toml format.')
    parser.add_argument("-s", "--sensors", default=8, type=int,
                        help='The total number of sensors to visualize.')
    args = parser.parse_args()

    # Load HopsWorks Kafka configuration
    conf = toml.load(args.config)

    # Kafka schema that this program supports/expects
    # The schema will be checked against the schema of the Kafka topic
    schema_str = """
    {
      "type": "record",
      "name": "sensor",
      "fields": [
        {
          "name": "timestamp",
          "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
          }
        },
        {
          "name": "id",
          "type": "string"
        },
        {
          "name": "value",
          "type": "double"
        }
      ]
    }
    """

    # url for the schema registry in HopsWorks REST API services
    registry_url = 'https://' + conf['hops']['url']\
        + conf['api']['base'] + '/project/'+conf['project']['id']+'/kafka'

    # Initialise the Confluent schema registry client
    schema_registry_conf = {'url': registry_url, 'ssl.ca.location': conf['hops']['verify']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Add the API key required by HopsWorks but not configurable through the confluent schema registry client
    headers = {'Authorization': 'ApiKey ' + conf['api']['key']}
    schema_registry_client._rest_client.session.headers.update(headers)

    # Initialize the avro deserializer for the value using the schema
    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_event)

    # Initialize a simple String deserializer for the key
    string_deserializer = StringDeserializer('utf_8')

    # Initialize the consumer
    consumer_conf = {'bootstrap.servers': conf['hops']['url']+':'+conf['kafka']['port'],
                     'security.protocol': 'SSL',
                     'ssl.ca.location': conf['project']['ca_file'],
                     'ssl.certificate.location': conf['project']['certificate_file'],
                     'ssl.key.location': conf['project']['key_file'],
                     'ssl.key.password': conf['project']['key_password'],
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': conf['kafka']['consumer']['group_id'],
                     'auto.offset.reset': conf['kafka']['consumer']['auto_offset_reset'],
                     }
    consumer = DeserializingConsumer(consumer_conf)
    # Subscribe to a topic
    consumer.subscribe([conf['kafka']['topic']])

    # a list of buffers to store data for plotting
    MAX_BUFFER = 1000  # max events to store for plotting, then graph will scroll
    buffer = [deque(maxlen=MAX_BUFFER) for x in range(args.sensors)]

    # Plotting
    fig, ax = plt.subplots(len(buffer), sharex=True)
    lines = [a.plot([])[0] for a in ax]
    plt.show(block=False)

    def plot():
        # x is shared, so set lim once
        ax[0].set_xlim(0, max(len(b) for b in buffer)+10)

        for b, l, a in zip(buffer, lines, ax):
            if len(b) == 0:
                continue

            # sort the buffer by timestamp
            sb = [e.value for e in sorted(b, key=lambda e: e.timestamp)]
            l.set_data(range(len(sb)),  sb)
            a.set_ylim(min(sb)-10, max(sb)+10)
        fig.canvas.draw()
        fig.canvas.flush_events()

    # loop for consuming events
    time = datetime.now()         # time for replotting every delta seconds
    delta = timedelta(seconds=0.5)
    while True:
        try:
            # plot
            if datetime.now() - time > delta:
                time = datetime.now()
                plot()

            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            event = msg.value()
            if event is not None:
                print("Event record {}: id: {}\n"
                      "\ttimestamp: {}\n"
                      "\tvalue: {}\n"
                      .format(msg.key(), event.id,
                              event.timestamp,
                              event.value))
                # store event in buffer for plotting
                id = int(event.id[6:])
                buffer[id].append(event)

        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    main()
