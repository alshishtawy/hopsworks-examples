from confluent_kafka import Producer
import toml
import argparse


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def main():

    # Parse arguments
    parser = argparse.ArgumentParser(description='A simple Producer example to produce strings'
                                     ' into a kafka topic hosted at a HopsWorks cluster.')
    parser.add_argument("-c", "--config", default='config.toml',
                        help='Configuration file in toml format.')
    args = parser.parse_args()

    # Load HopsWorks Kafka configuration
    conf = toml.load(args.config)

    # Initialize the Producer
    producer_conf = {'bootstrap.servers': conf['hops']['url']+':'+conf['kafka']['port'],
                     'security.protocol': 'SSL',
                     'ssl.ca.location': conf['project']['ca_file'],
                     'ssl.certificate.location': conf['project']['certificate_file'],
                     'ssl.key.location': conf['project']['key_file'],
                     'ssl.key.password': conf['project']['key_password']}
    p = Producer(producer_conf)


    for data in "Hello Kafka! I'm a simple client sending in some strings.".split():
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        p.produce(conf['kafka']['topic'], data.encode('utf-8'), callback=delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
    p.flush()


if __name__ == '__main__':
    main()
