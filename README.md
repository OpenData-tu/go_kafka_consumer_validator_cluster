# go_kafka_consumer_validator_cluster
a kafka consumer, that validates and pushes to elastic. Based on kafka consumer groups.

## Get it from docker-hub
[https://hub.docker.com/r/tubpaul/go_consumer/](https://hub.docker.com/r/tubpaul/go_consumer/)

## Configurable Varfiables
The go programm takes following env-variables

*Note:* actually there can be several brokers. Throuh env-variables currently only one can be set

`BROKER_URL`
 - url (including port) of kafka broker (e.g. `'example.com:9092'`)

`TOPICS`
 - kafka topics, that the consumer will listen to. separated by `;` (e.g. `greetings;test;topic1;topic2`). **NOTE:** the conumer can listen to many topics. BUT: if many topics means many data_sources/importer, the requests to elasticSearch will be messed up because the consumer assumes to work for 1 datasource only.


`ELASTIC_URL`
 - url, where the json body will be sent (ElasticSearch instance in our case), if validation is succesful. 

`CONSUMER_GROUP`
 - kafka consumer group, that the consumer will join

`DATASOURCE_ID`
 - the id of the datasource whose outputs the consumer validates and inserts. is needed to build the insertion url for elasticSearch (the id is in the url as index)

`BULK_LIMIT`
 - limit of jsons that get aggregated to one bulk request. E.g. if set to 100, the consumer will aggregate *100* JSONs it reads from the kafka to one JSON that gets inserted into ElasticSearrch via the REST-api

`TIMEOUT_SECONDS`
- sets a timeout in seconds after which the consumer will send aggregated JSONs anyways. Especially needed in order to send the last JSONs coming from the importer.

`DEBUG` *(optional)*
 - if set to `true` program will print additional debug *on startup* and *the repsonse status of the http request to elastic search.* Be sure it actually matches the string `'true'`.

`DEBUG_INFO` *(optional)*
 - if set to `true` program will print every debug message possible, including received jsons etc. Be sure it actually matches the string `'true'`.

 ## Run the container locally
 The environment variables can be set in different ways (env-file, within docker-file or in the command itself). If done via the command line, it should look something like this
 
```
docker run --env BROKER_URL='192.168.99.100:9092' --env TOPICS='greetings;test;topic1;topic2' --env ELASTIC_URL='https://requestb.in/1lz8nqi1' --env DEBUG='true' --env CONSUMER_GROUP='cgroup1' --env DATASOURCE_ID='de_blume' --env BULK_LIMIT='100' --env TIMEOUT_SECONDS='10' tubpaul/go_consumer
 ```
