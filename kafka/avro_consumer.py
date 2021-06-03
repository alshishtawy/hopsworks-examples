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
from confluent_kafka.schema_registry import record_subject_name_strategy
from datetime import datetime
import toml


class Sensor(object):
    """
    Sensor record

    Args:
        id (str): Sensor's id

        timestamp (datetime): timestamp in milliseconds

        value (double): Sensor's reading value

    """
    def __init__(self, id, timestamp, value):
        self.id = id
        self.timestamp = timestamp
        self.value = value


def dict_to_sensor(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    """
    if obj is None:
        return None

    return Sensor(id=obj['id'],
                timestamp=datetime.fromtimestamp(obj['timestamp']),
                value=obj['value'])


def main():
    conf = toml.load('config_az.toml')

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
    registry_url = 'https://' + conf['hops']['url']\
        + conf['api']['base'] + '/project/'+conf['project']['id']+'/kafka'

    schema_registry_conf = {'url': registry_url, 'ssl.ca.location': conf['hops']['verify']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # forcefully add the API key required by hops and not configurable through the confluent schema registry client
    headers={'Authorization': 'ApiKey ' + conf['api']['key']}

    schema_registry_client._rest_client.session.headers.update(headers)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_sensor)

    string_deserializer = StringDeserializer('utf_8')

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
    consumer.subscribe([conf['kafka']['topic']])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            sensor = msg.value()
            if sensor is not None:
                print("Sensor record {}: id: {}\n"
                      "\ttimestamp: {}\n"
                      "\tvalue: {}\n"
                      .format(msg.key(), sensor.id,
                              sensor.timestamp,
                              sensor.value))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    main()
