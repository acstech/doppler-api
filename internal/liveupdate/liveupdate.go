package liveupdate

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clientConnections map[string]map[*ConnWithParameters]struct{}
var count int = 0

type ConnWithParameters struct {
	ws            *websocket.Conn
	clientID      string
	filterSetting string
}

type Point struct {
	Latitude  string `json:"lat"`
	Longitude string `json:"lng"`
	Count     string `json:"count"`
	ClientID  string `json:"clientID"`
	EventID   string `json:"eventID"`
	// Insert time eventually
}

// Initialize listening for websocket requests
func InitWebsockets() {
	//intialize connection management
	clientConnections = make(map[string]map[*ConnWithParameters]struct{})
	fmt.Println("Ready to Receive Websocket Requests")
	//handle any websocket requests
	http.HandleFunc("/receive/ws", createWS)
	//listen for calls to server
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal(err)
	}
}

// Get a request for a point, then send coordinates back to front end
func createWS(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP server connection to the WebSocket protocol
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Connection Upgrade Error")
		fmt.Println(err)
		ws.Close() //close the connection just in case
		return
	}
	fmt.Println("Connection Upgraded")
	//Initialize conn with parameters
	conn := &ConnWithParameters{
		ws:            ws,
		clientID:      "",
		filterSetting: "",
	}

	//now listen for messages for this created websocket
	go readWS(conn)
}

func readWS(conn *ConnWithParameters) {
	defer conn.ws.Close()
	connected := false
	// Function to read any messages that are received
	for {
		//read messages from client
		_, msg, err := conn.ws.ReadMessage()
		//check if client closed connection
		if err != nil {
			fmt.Println("Connection Closed by Client")
			conn.ws.Close()
			return
		}
		// if websocket hasnt been intialized
		if connected == false {
			clientConnections[msg.clientID]
		}
	}
}

// Send points to front end
func SendPoint(point Point, cs *websocket.Conn) {
	for _, conn := range Websockets {
		//conn.WriteJSON(point)
		conn.addr.WriteJSON(Point{
			Longitude: point.Longitude,
			Latitude:  point.Latitude,
			Count:     "3",
		})
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
				// Check if ClientID exists
				if _, contains := Websockets[point.Client]; contains {
					// If it does create an instance of ConnectionStruct
					cs := Websockets[point.Client]

					if cs.filterSetting == "all" {
						point.Count = "3"
						SendPoint(point, cs.addr)
						// Match filter setting to point event
					} else if cs.filterSetting == point.Event {
						point.Count = "3"
						SendPoint(point, cs.addr)
					}
				}
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
