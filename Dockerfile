FROM golang:1.8

# Install beego and the bee dev tool
RUN go get github.com/xeipuuv/gojsonschema && go get github.com/bsm/sarama-cluster

RUN mkdir -p /app

ADD . /app/

WORKDIR /app

# ENV BROKER_URL 192.168.99.100:9092
# ENV TOPICS greetings;abc;def;hallo;baby
# ENV ELASTIC_URL https://requestb.in/ozcmitoz
# ENV DEBUG true
# ENV CONSUMER_GROUP cgroup1
# ENV DATASOURCE_ID de_blume
# ENV BULK_LIMIT 7 

RUN go build cluster_consumer_validator.go

# CMD ["/app/consumer_validator 192.168.99.100:9092 greetings"]
CMD ["/app/cluster_consumer_validator"]