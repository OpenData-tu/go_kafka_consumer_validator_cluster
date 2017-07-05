# go_kafka_consumer_validator_cluster
a kafka consumer, that validates and pushes to elastic. Based on kafka consumer groups.

## Get it from docker-hub
[https://hub.docker.com/r/tubpaul/go_consumer/](https://hub.docker.com/r/tubpaul/go_consumer/)

## Configurable Varfiables
The go programm takes following env-variables

*Note:* actually there can be several brokers and topics. Throuh env-variables currently only one can be set

`BROKER_URL`
url (including port) of kafka broker

`TOPIC`
kafka topic, that the consumer will listen to

`ELASTIC_URL`
url, where the json body will be sent, if validation is succesful (does not have to be elastic search instance)

`CONSUMER_GROUP`
kafka consumer group, that the consumer will join

`DEBUG` *(optional)*
if set to `true` program will print additional debug
