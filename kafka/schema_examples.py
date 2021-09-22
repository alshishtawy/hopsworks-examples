from hops import project
from hops import kafka
from hops import util, constants
from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
import requests
import toml
import argparse

### Examples on how to interact with HopsWorks Schema Registry service
### externally from outside the HopsWorks cluster to query the schema


### Hops Configuration

# Parse arguments
parser = argparse.ArgumentParser(description='Examples using different methods to access'
                                 ' HopsWorks Schema Registry externally from outside the cluster.')
parser.add_argument("-c", "--config", default='config.toml',
                        help='Configuration file in toml format.')
args = parser.parse_args()
conf = toml.load(args.config)

### Example 1
### Get schema using HopsWorks REST API
### https://app.swaggerhub.com/apis-docs/logicalclocks/hopsworks-api/2.2.0#/Project%20Service/getTopicSubject

print('Example 1: Using HopsWorks REST API')
print('===================================')
print()

# Security header with the API Key
headers={'Authorization': 'ApiKey ' + conf['api']['key']}

# list all available schemas for your project
print('list all available schemas for your project')
url = 'https://' + conf['hops']['url'] + conf['api']['base'] + '/project/' + conf['project']['id'] + '/kafka/subjects'
print('url: ' + url)
response = requests.get(url, headers=headers, verify=conf['hops']['verify'])
print('schemas: ' + response.text)
print()

# get the schema associated with a topic using the topic name
print('get the schema associated with a topic using the topic name')
url = 'https://' + conf['hops']['url'] + conf['api']['base'] + '/project/' + conf['project']['id'] \
                 + '/kafka/topics/'  + conf['kafka']['topic'] + '/subjects'
print('url: ' + url)
response = requests.get(url, headers=headers, verify=conf['hops']['verify'])
schema = response.json()['schema']

print('schema for topic {} using HopsWorks REST API:'.format(conf['kafka']['topic']))
print(schema)
print()


### Example 2
### Get schema the easy way
### Using hops-util-py
### For this to work you need to add "project" to your API key scope in HopsWorks settings so it includes both ["KAFKA","PROJECT"]

print('Example 2: Using hops-util-py')
print('=============================')
print()

# connect to the project to setup environment variables. Similar to what happens on Hopsworks cluster
# so you can use most of the hops util library
project.connect(conf['project']['name'], conf['hops']['url'], api_key=conf['api']['key_file'])

# get the schema associated with a topic using the topic name
print('get the schema associated with a topic using the topic name')

schema = kafka.get_schema(conf['kafka']['topic'])
print('schema for topic {} using Hops Util package:'.format(conf['kafka']['topic']))
print(schema)
print()

### Example 3
### Get the schema using the Confluent Schema Registry client

print('Example 3: Using the Confluent Schema Registry client')
print('=====================================================')
print()

registry_url = 'https://' + conf['hops']['url']\
    + conf['api']['base'] + '/project/'+conf['project']['id']+'/kafka'
print('registry url: ' + registry_url)

sc = SchemaRegistryClient({'url': registry_url, 'ssl.ca.location': conf['hops']['verify']})

# Add the API key required by HopsWorks but not configurable through the confluent schema registry client
sc._rest_client.session.headers.update(headers)

# here we must use the schema name to look it up as confluent allows multiple schemas per topic
# see: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#subject-name-strategy

print('get the schema using schema name')
schema = sc.get_latest_version(conf['kafka']['schema'])

print('id: {}'.format(schema.schema_id))
print('subject: {}'.format(schema.subject))
print('version: {}'.format(schema.version))
print('schema with confluent schema client:')
print(schema.schema.schema_str)
