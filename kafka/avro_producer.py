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


def sensor_to_dict(sensor, ctx):
    """
    Returns a dict representation of a Sensor instance for serialization.

    Args:
        sensor (Sensor): Sensor instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with sensor attributes to be serialized.

    """
    return dict(id=sensor.id,
                timestamp=datetime.timestamp(sensor.timestamp),
                value=sensor.value)


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
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('Sensor record {} successfully produced to {} [{}] at offset {}'.format(
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

    registry_url = 'https://' + conf['hops']['url']\
        + conf['api']['base'] + '/project/'+conf['project']['id']+'/kafka'

    schema_registry_conf = {'url': registry_url, 'ssl.ca.location': conf['hops']['verify']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # forcefully add the API key required by hops and not configurable through the confluent schema registry client
    headers={'Authorization': 'ApiKey ' + conf['api']['key']}
    schema_registry_client._rest_client.session.headers.update(headers)


    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     sensor_to_dict,
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

    print("Producing user records to topic {}. ^C to exit.".format(conf['kafka']['topic']))
    for x in range(0,10):
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            sensor = Sensor(id='s10',
                            timestamp=datetime.now(),
                            value=x/20.0)
            producer.produce(topic=conf['kafka']['topic'], key=str(uuid4()), value=sensor,
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    main()
