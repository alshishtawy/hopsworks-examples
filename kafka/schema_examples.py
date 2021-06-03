from hops import project
from hops import kafka
from hops import util, constants
from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
import requests
import toml

### Example on how to intract with Hopsworks Kafka service externally
### from outside the Hopsworks cluster to query the schema


### Hops Configration

conf = toml.load('config.toml')

### Get schema using Hops REST API
### https://app.swaggerhub.com/apis-docs/logicalclocks/hopsworks-api/2.2.0#/Project%20Service/getTopicSubject

# Security header with the API Key
headers={'Authorization': 'ApiKey ' + conf['api']['key']}

# list all available schemas for your project
url = 'https://' + conf['hops']['url'] + conf['api']['base'] + '/project/' + conf['project']['id'] + '/kafka/subjects'
print('url: ' + url)
response = requests.get(url, headers=headers, verify=conf['hops']['verify'])
print('schemas: ' + response.text)
print()

# get schema using topic name
url = 'https://' + conf['hops']['url'] + conf['api']['base'] + '/project/' + conf['project']['id'] \
                 + '/kafka/topics/'  + conf['kafka']['topic'] + '/subjects'
print('url: ' + url)
print()
response = requests.get(url, headers=headers, verify=conf['hops']['verify'])

schema = response.json()['schema']
print('schema with Hops REST API:')
print(schema)
print()


### Get schema the easy way
### Using hops-util-py

# connect to the project to setup environment variables. Similar to what happens on Hopsworks cluster
# so you can use most of the hops util library

# get the schema associated with a topic using the topic name
project.connect(conf['project']['name'], conf['hops']['url'], api_key=conf['api']['key_file'])
schema = kafka.get_schema(conf['kafka']['topic'])
print('schema with hops util:')
print(schema)
print()


### using confluent schema registry

registry_url = 'https://' + conf['hops']['url']\
    + conf['api']['base'] + '/project/'+conf['project']['id']+'/kafka'

print('registry url: ' + registry_url)
sc = SchemaRegistryClient({'url': registry_url, 'ssl.ca.location': conf['hops']['verify']})

# forcefully add the API key required by hops and not configurable through the confluent schema registry client
sc._rest_client.session.headers.update(headers)

# here we must use the schema name to look it up as confluent allows multiple schemas per topic
# see: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#subject-name-strategy

schema = sc.get_latest_version(conf['kafka']['schema'])

print('id: {}'.format(schema.schema_id))
print('subject: {}'.format(schema.subject))
print('version: {}'.format(schema.version))
print('schema with confluent schema client:')
print(schema.schema.schema_str)
