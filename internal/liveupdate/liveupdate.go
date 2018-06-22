package liveupdate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var cbConn *couchbase.Couchbase
var clientConnections map[string]map[*ConnWithParameters]struct{}
var mutex = &sync.RWMutex{}
var count int = 0

type ConnWithParameters struct {
	ws            *websocket.Conn     //keeps up with connection identifier
	clientID      string              //the clientID associated with this connection
	filterSetting map[string]struct{} //a map of the events that the client currently wants to see
}

type msg struct {
	ClientID      string
	FilterSetting []string `json:"filterSetting,omitempty"`
	//startTime <type> `json:"startTime, omitempty"`
	//endTime <type> `json:"endTime, omitempty"`
}

type KafkaData struct {
	Latitude  string `json:"lat"`
	Longitude string `json:"lng"`
	Count     string `json:"count"`
	ClientID  string `json:"clientID"`
	EventID   string `json:"eventID"`
	// Insert time eventually
}

// Initialize listening for websocket requests
func InitWebsockets(cbConnection string) {
	//create CB connection
	cbConn = &couchbase.Couchbase{Doc: &couchbase.Doc{}}
	cbConn.ConnectToCB(cbConnection)
	fmt.Println("Connected to Couchbase")
	fmt.Println()

	//start kafka consume
	go Consume()

	//server index
	http.HandleFunc("/", serveIndex)

	//intialize connection management
	clientConnections = make(map[string]map[*ConnWithParameters]struct{})
	fmt.Println("Ready to Receive Websocket Requests")
	fmt.Println()

	//handle any websocket requests
	http.HandleFunc("/receive/ws", createWS)
}

// Get a request for a point, then send coordinates back to front end
func createWS(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP server connection to the WebSocket protocol
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Connection Upgrade Error")
		fmt.Println()
		fmt.Println(err)
		ws.Close() //close the connection just in case
		return
	}
	fmt.Println("NEW CONNECTION: Connection Upgraded")
	fmt.Println()

	//Initialize conn with parameters
	conn := &ConnWithParameters{
		ws:            ws,
		clientID:      "",
		filterSetting: make(map[string]struct{}),
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
		_, msgBytes, err := conn.ws.ReadMessage()
		conn.ws.WriteJSON("got message")
		//check if client closed connection
		if err != nil {
			fmt.Println("Connection Closed by Client")
			//REMOVE FROM MAP
			mutex.Lock()
			delete(clientConnections[conn.clientID], conn)
			fmt.Println(clientConnections)
			mutex.Unlock()
			return
		}
		var message msg
		//unmarshal (convert bytes to msg struct)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			fmt.Println("unmarshal error")
			fmt.Println(err)
		}
		fmt.Println("Message Received from ", message.ClientID, " on connection ", conn)
		fmt.Println("json msg: ", message)
		fmt.Println()

		// if websocket hasnt been added to clientConnections map
		if connected == false {
			//update conn with new parameters
			conn.clientID = message.ClientID
			//UPDATE FILTERSETTINGS
			conn.filterSetting = make(map[string]struct{})
			for _, event := range message.FilterSetting {
				conn.filterSetting[event] = struct{}{}
			}
			fmt.Println("filter setting: ", conn.filterSetting)

			//check if client is already connected on another websocket
			//if client has not been connected, create new connection map
			mutex.RLock()
			if _, contains := clientConnections[message.ClientID]; !contains {
				clientConnections[message.ClientID] = make(map[*ConnWithParameters]struct{})
			}
			mutex.Unlock()
			//add conn to map
			mutex.Lock()
			clientConnections[message.ClientID][conn] = struct{}{}
			fmt.Println("added client", clientConnections)
			mutex.Unlock()
			fmt.Println()
			connected = true

			//CHECK COUCHBASE
			//check if client exists in couchbase
			if cbConn.ClientExists(message.ClientID) {
				//query couchbase for client's events
				clientEvents := cbConn.Doc.Events
				conn.ws.WriteJSON(clientEvents)
			} else {
				//if clientID does not exist in couchbase
				conn.ws.WriteMessage(1, []byte("ClientID not found"))
			}

			//continue to next for loop iteration, skipping updating filters
			continue
		}
		//UPDATE FILTERS
		conn.filterSetting = make(map[string]struct{})
		for _, event := range message.FilterSetting {
			conn.filterSetting[event] = struct{}{}
		}

		fmt.Println("filter setting: ", conn.filterSetting)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

// Consume messages from queue
func Consume() {
	fmt.Println("Kafka Consume Started")
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
				// fmt.Println(string(msg.Value))
				var kafkaData KafkaData
				json.Unmarshal(msg.Value, &kafkaData)

				// fmt.Println("Lat: " + kafkaData.Latitude + " Lng: " + kafkaData.Longitude)
				// Check if ClientID exists
				mutex.RLock()
				if _, contains := clientConnections[kafkaData.ClientID]; contains {
					// If client is connected, get map of connections
					clientConnections := clientConnections[kafkaData.ClientID]
					//iterate over client connections
					for conn, _ := range clientConnections {
						//if connection filter has KafkaData eventID, send data
						if _, hasEvent := conn.filterSetting[kafkaData.EventID]; hasEvent {
							conn.ws.WriteJSON(kafkaData)
							//fmt.Println("Send Data to ", conn.clientID)
						}
					}
				}
				mutex.Unlock()
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
