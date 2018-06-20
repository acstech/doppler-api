package liveupdate

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/websocket"
	"net/http"
	"os"
	"os/signal"
	"strconv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var Websockets map[string]*websocket.Conn
var count int = 0

type Point struct {
	Latitude  string `json:"lat"`
	Longitude string `json:"lng"`
	Count     string `json:"count"`
	// Insert time eventually
}

// Start a websocket connection for client
func StartWebsocket() {
	http.HandleFunc("/recieve/ws", dot)
	Websockets = make(map[string]*websocket.Conn)
	fmt.Println("websocket running!")
	http.ListenAndServe(":8000", nil)
}

// Get a request for a point, then send coordinates back to front end
func dot(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP server connection to the WebSocket protocol
	var conn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Function to read any messages that are received
	go func(conn *websocket.Conn) {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				return
			}
		}
	}(conn)

	Websockets[strconv.Itoa(count)] = conn
	count++
	fmt.Println(count)
}

// Send points to front end
func SendPoint(point Point) {
	for _, conn := range Websockets {
		conn.WriteJSON(point)
	}
}

// Consume messages from queue
func Consume() {
	// Create a new configuration instance
	config := sarama.NewConfig()

	// Specify brokers address. 9092 is default
	brokers := []string{"localhost:9092"}

	// Create a new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	// Wait to close after everything is processed
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	// Topic to consume
	topic := "influx-topic"

	// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given offset
	// A PartitionConsumer processes messages from a given topic and partition
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	// Stop process if connection is interrupted
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Signal to finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			// In case of error
			case err := <-consumer.Errors():
				fmt.Println(err)
			// Print consumer messages
			case msg := <-consumer.Messages():
				//fmt.Println(string(msg.Value))

				var point Point
				json.Unmarshal(msg.Value, &point)
				//fmt.Println("Lat: " + point.Latitude + " Lng: " + point.Longitude)
				SendPoint(point)
			// Service interruption
			case <-signals:
				fmt.Println("Interrupt detected")
				doneCh <- struct{}{}
			}
		}
	}()

	// If everything is done, close consumer
	<-doneCh
	fmt.Println("Consumption closed")
}

// func main() {
// 	go StartWebsocket()
// 	Consume()
// }
