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
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/acstech/doppler-api/internal/service"
	influx "github.com/influxdata/influxdb/client/v2"
	_ "github.com/joho/godotenv/autoload"
)

func main() {

	// get environment variables
	// get couchbase connection
	cbEnv := os.Getenv("COUCHBASE_CONN")

	// get and parse kafka env variables
	kafkaCon, kafkaTopic, err := kafkaParse(os.Getenv("KAFKA_CONN"))
	if err != nil {
		fmt.Println("kafka parse error: ", err)
	}

	// get influxDB env variables
	influxCon := os.Getenv("CONNECTOR_CONNECT_INFLUX_URL")
	influxUser := os.Getenv("CONNECTOR_CONNECT_INFLUX_USERNAME")
	influxPassword := os.Getenv("CONNECTOR_CONNECT_INFLUX_PASSWORD")

	//connect to couchbase
	cbConn := &couchbase.Couchbase{} // create instance of couchbase connection
	err = cbConn.ConnectToCB(cbEnv)  // connect to couchbase with env variables
	if err != nil {
		panic(fmt.Errorf("error connecting to couchbase: %v", err))
	}
	fmt.Println("Connected to Couchbase")

	//connect to Kafka and create consumer
	consumer, err := createConsumer(kafkaCon, kafkaTopic) // create instance of consumer with env variables
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Connected to Kafka")

	// creates influx client
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     influxCon,
		Username: influxUser,
		Password: influxPassword,
	})
	if err != nil {
		panic(fmt.Errorf("error connecting to influx: %v", err))
	}
	fmt.Println("Connected to InfluxDB")
	fmt.Println()

	// intialize websocket management and kafka consume
	// connectionManager requires maxBatchSize, minBatchSize, batchInterval (in milliseconds), truncateSize, cbConn
	maxBatchSize := 100
	minBatchSize := 1
	batchInterval := 2000
	truncateSize := 1

	// listen for service interrupt signal
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// create context for doppler-api service
	ctx, cancel := context.WithCancel(context.Background()) // create a context that utilizes a Done channel, returns context and cancel function that is used to signal the Done channel

	// go func that listens for signals
	go func() {
		<-quit // signal channel

		log.Println("Interrupt Received")
		cancel() // send signal to Done channel

		// close internal services
		cbConn.Bucket.Close()
		consumer.Close()
		c.Close()
		fmt.Println("Internal Services Closed")
	}()

	// create an instance of our websocket service
	connectionManager := service.NewConnectionManager(maxBatchSize, minBatchSize, batchInterval, truncateSize, cbConn)
	influxService := service.NewInfluxService(c, truncateSize)

	// handle websocket requests
	http.Handle("/receive/ws", connectionManager)

	// handle ajax requests
	http.Handle("/receive/ajax", influxService)

	// start the consumer
	go connectionManager.Consume(ctx, consumer)

	// create instance of server
	server := &http.Server{Addr: ":8000"}

	// go func that listens and serves doppler-api server
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			panic(fmt.Errorf("error setting up the websocket endpoint: %v", err))
		}
	}()

	<-ctx.Done() // context Done channel endpoint

	// received an interrupt signal, shut down.
	// create context for server shutdown
	svrCtx, svrCancel := context.WithTimeout(context.Background(), 5*time.Second)
	// shutdown server with server context
	if err := server.Shutdown(svrCtx); err != nil {
		// Error from closing listeners, or context timeout:
		log.Printf("HTTP server Shutdown: %v", err)
	}
	defer svrCancel() // defer signaling server context Done channel signal
	fmt.Println("Service Closed")
}

// kafkaParse is used to parse env variables for Kafka
func kafkaParse(conn string) (string, string, error) {
	u, err := url.Parse(conn)
	if err != nil {
		return "", "", err
	}
	if u.Host == "" {
		return "", "", errors.New("Kafka address is not specified, verify that your environment variables are correct")
	}
	address := u.Host
	// make sure that the topic is specified
	if u.Path == "" || u.Path == "/" {
		return "", "", errors.New("Kafka topic is not specified, verify that your environment variables are correct")
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
