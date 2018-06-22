package liveupdate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/gorilla/websocket"
)

// Force origin policy to be true in order to avoid 403 error
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Manage clients, connections, filters of those connections
var Websockets = make(map[string]map[*Connection]struct{})
var count int = 0
var cbConn *couchbase.Couchbase

// Connection has a websocket connection and a filter setting
type Connection struct {
	addr          *websocket.Conn
	filterSetting string
}

var connection Connection

// Data to be sent to front end so it can be mapped
type Point struct {
	Latitude  string `json:"lat,omitempty"`
	Longitude string `json:"lng,omitempty"`
	// Number of times this event has happened
	Count  string `json:"count,omitempty"`
	Client string `json:"clientID,omitempty"`
	Event  string `json:"eventID,omitempty"`
	// Insert time eventually
}

// Start a websocket connection for client
// cbCon is the couchbase connection string
func StartWebsocket(cbCon string) {
	//create CB connection
	cbConn = &couchbase.Couchbase{Doc: &couchbase.Doc{}}
	// Connect to couchbase
	cbConn.ConnectToCB(cbCon)
	http.HandleFunc("/v3/ws", dot)
	Websockets = make(map[string]map[*Connection]struct{})
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
			_, msg, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				return
			}

			if strings.Contains(string(msg), "clientID") {
				var point Point
				// Unmarshal read message into point object
				err := json.Unmarshal(msg, &point)
				if err != nil {
					panic(err)
				}
				// See if client id exists by running couchbase query
				if cbConn.ClientExists(point.Client) {
					// Pass client id as a key value
					Websockets[point.Client] = make(map[*Connection]struct{})
					err = conn.WriteJSON(cbConn.Doc.Events)
					if err != nil {
						panic(err)
					}
					fmt.Println("connected")
				}
			} else {
				conn.WriteMessage(1, msg)
			}
		}
	}(conn)

	// Websockets[strconv.Itoa(count)] = conn
	// count++
	// fmt.Println(count)
}

// Send points to front end
// func SendPoint(point Point, connection Connection) {
// 	for _, conn := range Websockets {
// 		//conn.WriteJSON(point)
// 		connection.addr.WriteJSON(Point{
// 			Longitude: point.Longitude,
// 			Latitude:  point.Latitude,
// 			Count:     "3",
// 		})
// 	}
// }

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
				// if _, contains := Websockets[point.Client]; contains {
				// 	// If it does create an instance of ConnectionStruct
				// 	cs := Websockets[point.Client]

				// 	if cs.filterSetting == "all" {
				// 		point.Count = "3"
				// 		SendPoint(point, cs.addr)
				// 		// Match filter setting to point event
				// 	} else if cs.filterSetting == point.Event {
				// 		point.Count = "3"
				// 		SendPoint(point, cs.addr)
				// 	}
				// }
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
