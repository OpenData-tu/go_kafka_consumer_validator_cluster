package main

import (
    "fmt"
    "os"
    "os/signal"
    "net/http"
    "bytes"
    "io/ioutil"
    "regexp"
    // "strings"
    // "strconv"

    "github.com/xeipuuv/gojsonschema"
    // "github.com/Shopify/sarama"
    cluster "github.com/bsm/sarama-cluster"
)

func main() {
    // now for testing, will be queried via http later
    schemaLoader := gojsonschema.NewStringLoader(`{}`)
    // schemaLoader := gojsonschema.NewStringLoader(`{"$schema": "http://json-schema.org/schema#","title": "Data Source","description": "A Data Source for Open Sensor Data from the CP project at TU Berlin. ","type": "object","properties": {"source_id": {  "type": "string"    },  "device": {  "type": "string"    },  "timestamp": {  "type": "string",    "format": "date-time"    },  "timestamp_record": {  "type": "string",    "format": "date-time"    },  "location": {  "type": "object",    "properties": {    "lat": {      "type": "number",          "exclusiveMaximum": true,          "exclusiveMinimum": true,          "maximum": 90,          "minimum": -90,      },      "lon": {      "type": "number",        "exclusiveMaximum": true,"exclusiveMinimum": true,"maximum": 180,"minimum": -180,}},"required": ["lat", "lon"]    },  "license": {  "type": "string"    },  "sensors": {  "type": "object",    "items": [{"type": "object","properties": {"sensor": {"type": "string"},"observation_value": {"type": "number"}}}]}},"required": ["timestamp","sensors", "location", "license"]}`)

    // if true, debug outputs will be printed. errors are printed anyways
    var debug bool = os.Getenv("DEBUG") == "true"
    // var debug bool = true
    if debug {
        fmt.Println("In DEBUG modus")
    }

    var messageCountStart int = 0

    var topics := strings.Split(os.Getenv("TOPICS"), ";")
    // topics := []string {"greetings"}
    if debug {
        fmt.Println("Listening on topics:")
        for _, element := range topics {
            fmt.Println(element)
        }
    }

    var broker string = os.Getenv("BROKER_URL")
    // var broker string = "192.168.99.100:9092"
    if debug {
        fmt.Println("Kafka Broker url: " + broker)
    }

    var consumer_group string = os.Getenv("CONSUMER_GROUP")
    // var consumer_group string = "cgroup1"
    if debug {
        fmt.Println("Consumer Group: " + consumer_group)
    }

    // var bulk_limit int = 10
    var bulk_limit int = os.Getenv("BULK_LIMIT")
    if debug {
        fmt.Println("Set bulk insertion limit to: " + string(bulk_limit))
    }


    // var data_source_id string = "de_blume_messnetz" 
    var data_source_id string = os.Getenv("DATASOURCE_ID")
    if debug {
        fmt.Println("Data Source ID: " + data_source_id)
    }

    // var counter int = 0
    // url := "http://ec2-13-59-103-133.us-east-2.compute.amazonaws.com:9200/weather/luftdaten/"
    // url := "https://requestb.in/wjmpahwj"
    url := os.Getenv("ELASTIC_URL")
    if debug {
        fmt.Println("ElasticSearch url: " + url)
    }
    // url += "/" + data_source_id + "/" + data + "/_bulk"




    brokers := []string {broker}

    config := cluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Group.Return.Notifications = true

    // topics := []string{topic}
    consumer, err := cluster.NewConsumer(brokers, consumer_group, topics, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // trap SIGINT to trigger a shutdown.
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    

    doneCh := make(chan struct{})


    requestChannel := make(chan string)
    go func() {
        if debug {
            fmt.Println("Start listening for aggregated jsons to push via bulk ..... ")
        }

        // listen for validated json
        for {
            select{
                case x := <- requestChannel:
                    if debug {
                        fmt.Println("received aggregated JSON out of channel " + x)
                    }
                    buf := bytes.NewBufferString(x)
                    req, err := http.NewRequest("PUT", url, buf)
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
            }
        }

    }()



    jsonChannel := make(chan string)
    go func() {
        if debug {
            fmt.Println("Start listening for validated json ..... ")
        }
        var aggregated_json string = ""
        var json_counter = 0
        var index_info string = "{\"index\":  {}}\n"
        // newlines have to be removed from json
        re := regexp.MustCompile(`\r?\n`)
        // listen for validated json
        for {
            select{
                case x := <- jsonChannel:
                    fmt.Println("received JSON out of channel " + x)
                    aggregated_json += index_info
                    aggregated_json += re.ReplaceAllString(x, " ") + "\n"
                    json_counter += 1
                    // if bulk limit is reached send json to goroutine that does the request
                    if json_counter >= bulk_limit {
                        requestChannel <- aggregated_json
                        json_counter = 0
                        aggregated_json = ""
                    }
            }
        }

    }()


    go func() {
        if debug {
            fmt.Println("Ready Configuring. Start to listen to kafka ..... ")
        }
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
                        }

                        jsonChannel <- string(msg.Value[:])
                        // counter++

                    } else {
                        fmt.Printf("The document is not valid. see errors :\n")
                        for _, desc := range result.Errors() {
                            fmt.Printf("- %s\n", desc)
                        }
                    }
                } else {
                        fmt.Printf("An error occured while validating the json\n")
                        fmt.Println(err)
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