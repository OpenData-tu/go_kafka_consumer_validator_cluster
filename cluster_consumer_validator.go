package main

import (
    "fmt"
    "os"
    "os/signal"
    "net/http"
    "bytes"
    "io/ioutil"
    // "strconv"

    "github.com/xeipuuv/gojsonschema"
    // "github.com/Shopify/sarama"
    cluster "github.com/bsm/sarama-cluster"
)

func main() {
    // args := os.Args[1:]
    // if len(args) != 2 {
    //     panic("usage: <cmd> broker_address topic")
    // }

    
    // now for testing, will be queried via http later
    schemaLoader := gojsonschema.NewStringLoader(`{"title": "Data Source", "description": "A Data Source for Open Sensor Data from the CP project at TU Berlin. ", "type": "object", "properties": { "source_id": {"enum": ["luftdaten_info"]}, "device": {"type": "string"}, "timestamp": { "type": "string", "format": "date-time" }, "timestamp_data": { "type": "string", "format": "date-time" }, "location": { "type": "object", "properties": { "lat": {"type": "number"}, "lon": {"type": "number"}          }        },        "license": {"type": "string"},        "sensors": {          "type": "object",          "items": [          { "type": "object", "properties": { "sensor": {"type": "string"}, "observation_type": {"type": "string"}, "observation_value": {"type": "number"}            }          }]        }      }    }`)

    // if true, debug outputs will be printed. errors are printed anyways
    var debug bool = os.Getenv("DEBUG") == "true"
    // var debug bool = true

    var messageCountStart int = 0
    var topic string = os.Getenv("TOPIC")
    // var topic string = "greetings"
    // var topic string = args[1]
    var broker string = os.Getenv("BROKER_URL")
    // var broker string = "192.168.99.100:9092"
    // var broker string = args[0]

    var consumer_group string = os.Getenv("CONSUMER_GROUP")

    // var counter int = 0
    // url := "http://ec2-13-59-103-133.us-east-2.compute.amazonaws.com:9200/weather/luftdaten/"
    url := "https://requestb.in/ozcmitoz"
    // url := os.Getenv("ELASTIC_URL")

    brokers := []string {broker}

    config := cluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Group.Return.Notifications = true

    topics := []string{topic}
    consumer, err := cluster.NewConsumer(brokers, consumer_group, topics, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // trap SIGINT to trigger a shutdown.
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    doneCh := make(chan struct{})
    go func() {
        for {
            select {
            case err := <-consumer.Errors():
                fmt.Println(err)
            case msg := <-consumer.Messages():
                messageCountStart++
                // fmt.Println("Received messages", string(msg.Key), string(msg.Value))
                if debug {
                    fmt.Println("Received messages")
                }

                stringLoader := gojsonschema.NewStringLoader(string(msg.Value[:]))
                result, err := gojsonschema.Validate(schemaLoader, stringLoader)
                if err == nil {
                    if result.Valid() {

                        if debug {
                            fmt.Printf("The document is valid\n")
                            // fmt.Println(counter)
                            fmt.Println("Sending request to: " + url)
                        }

                        // currently no index, if there was, it would have to be read from the json
                        // req, err := http.NewRequest("PUT", url + strconv.Itoa(counter), bytes.NewBuffer(msg.Value))
                        req, err := http.NewRequest("PUT", url, bytes.NewBuffer(msg.Value))
                        req.Header.Set("Content-Type", "application/json")

                        client := &http.Client{}
                        resp, err := client.Do(req)
                        if err != nil {
                            panic(err)
                        }
                        defer resp.Body.Close()

                        if debug {
                            fmt.Println("response Status:", resp.Status)
                            fmt.Println("response Headers:", resp.Header)
                            body, _ := ioutil.ReadAll(resp.Body)
                            fmt.Println("response Body:", string(body))
                        }
                        // counter++

                    } else {
                        fmt.Printf("The document is not valid. see errors :\n")
                        for _, desc := range result.Errors() {
                            fmt.Printf("- %s\n", desc)
                        }
                    }
                } else {
                        fmt.Printf("An error occured while validating the\n")
                }
            case <-signals:
                fmt.Println("Interrupt is detected")
                doneCh <- struct{}{}
            }
        }
    }()
    <-doneCh
    fmt.Println("Processed", messageCountStart, "messages")
}