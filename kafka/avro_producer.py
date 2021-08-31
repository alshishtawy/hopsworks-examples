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
from uuid import uuid4

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import record_subject_name_strategy
from datetime import datetime
import toml
from sensor import sensor
from time import sleep

class Event(object):
    """
    An object representing a sensor event

    Args:
        id (str): Sensor's id

        timestamp (datetime): timestamp in milliseconds

        value (double): Sensor's reading value

    """
    def __init__(self, id, timestamp, value):
        self.id = id
        self.timestamp = timestamp
        self.value = value


def event_to_dict(event, ctx):
    """
    Returns a dict representation of a sensor Event instance for serialization.

    Args:
        event (Event): Event instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with sensor event attributes to be serialized.

    """
    return dict(id=event.id,
                timestamp=datetime.timestamp(event.timestamp),
                value=event.value)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for sensor Event {}: {}".format(msg.key(), err))
        return
    print('Sensor Event {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():

    conf = toml.load('config.toml')

    schema_str = """
    {
      "type": "record",
      "name": "sensor",
      "fields": [
        {
          "name": "timestamp",
          "type": "long",
          "logicalType": "timestamp-millis"
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

    schema_registry_conf = {'url': registry_url, 'ssl.ca.location': conf['hops']['verify']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # add the API key required by HopsWorks but not configurable through the confluent schema registry client
    headers={'Authorization': 'ApiKey ' + conf['api']['key']}
    schema_registry_client._rest_client.session.headers.update(headers)


    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     event_to_dict,
                                     {'auto.register.schemas': False, 'subject.name.strategy': record_subject_name_strategy})


    producer_conf = {'bootstrap.servers': conf['hops']['url']+':'+conf['kafka']['port'],
                     'security.protocol': 'SSL',
                     'ssl.ca.location': conf['project']['ca_file'],
                     'ssl.certificate.location': conf['project']['certificate_file'],
                     'ssl.key.location': conf['project']['key_file'],
                     'ssl.key.password': conf['project']['key_password'],
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)


    # initialize a number of sensors
    #start/end time step for the sensor data generator
    start = 0
    number_of_events = 1000
    end = start + number_of_events
    sensors = [
        sensor(baseline=10, slope=0.1,  period = 100, amplitude= 40, noise_level=5, start=start, end=end),
        sensor(baseline=10, slope=0.2,  period =  80, amplitude= 30, noise_level=4, start=start, end=end),
        sensor(baseline=20, slope=-0.1, period = 100, amplitude= 50, noise_level=6, phase=20, start=start, end=end),
        sensor(baseline=10, slope=0.1,  period = 100, amplitude= 40, noise_level=0, start=start, end=end),
        ]

    print("Producing sensor events to topic {}. ^C to exit.".format(conf['kafka']['topic']))
    print('Press Ctrl-c to stop')

    # a counter for the number of time steps generated
    time_step = start

    try:
        for data in zip(*sensors):
            timestamp=datetime.now()
            time_step += 1
            for i, d in enumerate(data):
                # Serve on_delivery callbacks from previous calls to produce()
                producer.poll(0.0)
                try:
                    event = Event(id='sensor'+str(i),
                                  timestamp=timestamp,
                                  value=d)
                    producer.produce(topic=conf['kafka']['topic'], key=event.id, value=event,
                                     on_delivery=delivery_report)
                except KeyboardInterrupt:
                    break
                except ValueError:
                    print("Invalid input, discarding record...")
                    continue
            sleep(0.5)
    except KeyboardInterrupt:
        print('\nStopping...')
        print('To continue execution start from event {}'.format(time_step))

    print("Flushing records...")
    producer.flush()


if __name__ == '__main__':
    main()
