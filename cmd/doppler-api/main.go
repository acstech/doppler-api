package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/acstech/doppler-api/internal/service"
	client "github.com/influxdata/influxdb/client/v2"
	_ "github.com/joho/godotenv/autoload"
)

func main() {

	// get environment variables
	cbEnv := os.Getenv("COUCHBASE_CONN")
	kafkaCon, kafkaTopic, err := kafkaParse(os.Getenv("KAFKA_CONN"))
	if err != nil {
		fmt.Println("kafka parse error: ", err)
	}
	influxCon := os.Getenv("CONNECTOR_CONNECT_INFLUX_URL")
	influxUser := os.Getenv("CONNECTOR_CONNECT_INFLUX_USERNAME")
	influxPassword := os.Getenv("CONNECTOR_CONNECT_INFLUX_PASSWORD")

	// creates influx client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     influxCon,
		Username: influxUser,
		Password: influxPassword,
	})
	if err != nil {
		panic(fmt.Errorf("error connecting to influx: %v", err))
	}
	defer func() {
		err = c.Close()
		if err != nil {
			fmt.Println("Closing InfluxDB Error: ", err)
		}
	}()

	//connect to couchbase
	cbConn := &couchbase.Couchbase{} // create instance of couchbase connection
	err = cbConn.ConnectToCB(cbEnv)  // connect to couchbase with env variables
	if err != nil {
		panic(fmt.Errorf("error connecting to couchbase: %v", err))
	}
	// close couchbase connection on return
	defer func() {
		err = cbConn.Bucket.Close()
		fmt.Println("Closed Couchbase")
		if err != nil {
			fmt.Println("Closing Couchbase Error: ", err)
		}
	}()

	fmt.Println("Connected to Couchbase")
	fmt.Println()

	//connect to Kafka and create consumer
	consumer, err := createConsumer(kafkaCon, kafkaTopic) // create instance of consumer with env variables
	if err != nil {
		fmt.Println(err)
	}

	// close consumer on return
	defer func() {
		err = consumer.Close()
		fmt.Println("Closed Kafka")
		if err != nil {
			fmt.Println("Closing Kafka Error: ", err)
		}
	}()

	//intialize websocket management and kafka consume
	// connectionManager requires maxBatchSize, minBatchSize, batchInterval (in milliseconds), truncateSize, cbConn
	maxBatchSize := 100
	minBatchSize := 1
	batchInterval := 2000
	truncateSize := 1

	// create an instance of our websocket service
	connectionManager := service.NewConnectionManager(maxBatchSize, minBatchSize, batchInterval, truncateSize, cbConn)
	influxService := service.NewInfluxService(c, truncateSize)

	// create new serve mux instance
	// mux := http.NewServeMux()

	// handle websocket requests
	http.Handle("/receive/ws", connectionManager)

	// handle ajax requests
	http.Handle("/receive/ajax", influxService)

	// start the consumer
	go connectionManager.Consume(consumer)

	// listen for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit)

	// listen for calls to server
	server := &http.Server{Addr: ":8000"}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			panic(fmt.Errorf("error setting up the websocket endpoint: %v", err))
		}
	}()

	<-quit
	// We received an interrupt signal, shut down.
	if err := server.Shutdown(context.Background()); err != nil {
		// Error from closing listeners, or context timeout:
		log.Printf("HTTP server Shutdown: %v", err)
	}
	fmt.Println("Service Closed")
}

// kafkaParse is used to parse env variables for Kafka
func kafkaParse(conn string) (string, string, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return "", "", err
	}
	if u.Host == "" {
		return "", "", errors.New("Kafka address is not specified, verify that your environment varaibles are correct")
	}
	address := u.Host
	// make sure that the topic is specified
	if u.Path == "" || u.Path == "/" {
		return "", "", errors.New("Kafka topic is not specified, verify that your environment varaibles are correct")
	}
	topic := u.Path[1:]
	return address, topic, nil
}

// createConsumer creates a new kafka consumer based on env variables
// returns a sarama.PartitionConsumer
func createConsumer(kafkaCon string, kafkaTopic string) (sarama.PartitionConsumer, error) {
	// Create a new configuration instance
	config := sarama.NewConfig()
	// Specify brokers address. 9092 is default
	brokers := []string{kafkaCon}

	// Create a new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given offset
	// A PartitionConsumer processes messages from a given topic and partition
	consumer, err := master.ConsumePartition(kafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// // queries influx for all points, query is temporarily hard coded
// influx := false
// if influx == true {
// 	res, err := fx.GetPoints(c, "select * from dopplerDataHistory")
// 	if err != nil {
// 		fmt.Println(err)
// 	} else {
// 		i := 0
// 		for i < len(res.ValArray) {
// 			fmt.Println(res.ValArray[i])
// 			i = i + 1
// 		}
// 	}
// }
