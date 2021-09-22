from confluent_kafka import Consumer
import toml
import argparse

def main():

    # Parse arguments
    parser = argparse.ArgumentParser(description='A simple Consumer example to consume strings'
                                     ' from a kafka topic hosted at a HopsWorks cluster.')
    parser.add_argument("-c", "--config", default='config.toml',
                        help='Configuration file in toml format.')
    args = parser.parse_args()

    # Load HopsWorks Kafka configuration
    conf = toml.load(args.config)

    # Initialize the Consumer
    consumer_conf = {'bootstrap.servers': conf['hops']['url']+':'+conf['kafka']['port'],
                     'security.protocol': 'SSL',
                     'ssl.ca.location': conf['project']['ca_file'],
                     'ssl.certificate.location': conf['project']['certificate_file'],
                     'ssl.key.location': conf['project']['key_file'],
                     'ssl.key.password': conf['project']['key_password'],
                     'group.id': conf['kafka']['consumer']['group_id'],
                     'auto.offset.reset': conf['kafka']['consumer']['auto_offset_reset'],
                     }

    consumer = Consumer(consumer_conf)

    # Subscribe to topics
    consumer.subscribe([conf['kafka']['topic']])

    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))
        except KeyboardInterrupt:
            break

    consumer.close()

if __name__ == '__main__':
    main()
