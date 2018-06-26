package liveupdate

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/gorilla/websocket"
)

//upgrader var used to set parameters for websocket connections
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var cbConn *couchbase.Couchbase                                   //used to hold couchbase connection
var clientConnections map[string]map[*ConnWithParameters]struct{} //map used as connection hub, keeps up with clients and their respective connections and each connections settings
var mutex = &sync.RWMutex{}                                       //mutex used for concurrent reading and writing
var count int = 0                                                 //hard coded weight
var maxBatchSize = 20                                             //max size of data batch that is sent
var minBatchSize = 1                                              //min size of data batch that is sent
var batchInterval = time.Duration(200 * time.Millisecond)         //millisecond interval that data is sent

//used to add parameters to a gorilla's websocket.Conn
type ConnWithParameters struct {
	ws         *websocket.Conn     //keeps up with connection identifier
	clientID   string              //the clientID associated with this connection
	filter     map[string]struct{} //a map of the events that the client currently wants to see
	allFilters map[string]struct{} //a map of all the events that the client has available
	batchArray []KafkaData         //array used to hold data for batch sending
}

type BatchStruct struct {
	BatchArray []KafkaData `json:"batchArray"`
}

//JSON format messages from client
type msg struct {
	ClientID string   `json:"clientID,omitempty"`
	Filter   []string `json:"Filter,omitempty"`
	//startTime <type> `json:"startTime, omitempty"`
	//endTime <type> `json:"endTime, omitempty"`
}

//JSON format messages from Kafka
type KafkaData struct {
	Latitude  string `json:"lat,omitempty"`
	Longitude string `json:"lng,omitempty"`
	Count     string `json:"count,omitempty"`
	ClientID  string `json:"clientID,omitempty"`
	EventID   string `json:"eventID,omitempty"`
	// Insert time eventually
}

// Initialize listening for websocket requests
func InitWebsockets(cbConnection string) {
	//create CB connection
	cbConn = &couchbase.Couchbase{Doc: &couchbase.Doc{}}
	cbConn.ConnectToCB(cbConnection)
	fmt.Println("Connected to Couchbase")
	fmt.Println()

	//intialize connection management
	clientConnections = make(map[string]map[*ConnWithParameters]struct{})
	fmt.Println("Ready to Receive Websocket Requests")
	fmt.Println()

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
	fmt.Println("NEW CONNECTION: Connection Upgraded, waiting for ClientID")

	//Initialize conn with parameters
	conn := &ConnWithParameters{
		ws:         ws,
		clientID:   "",
		filter:     make(map[string]struct{}),
		allFilters: make(map[string]struct{}),
	}

	//now listen for messages for this created websocket
	go readWS(conn)
}

func readWS(conn *ConnWithParameters) {
	defer conn.ws.Close()

	//boolean used to keep up if this websocket has been connected
	connected := false

	// Function to read any messages that are received
	for {
		//read messages from client
		_, msgBytes, err := conn.ws.ReadMessage()
		//check if client closed connection
		if err != nil {
			fmt.Println("Connection Closed by Client")
			//REMOVE FROM MAP
			mutex.Lock()
			delete(clientConnections[conn.clientID], conn)
			//check if client has any remaining connections, if so, delete client
			if len(clientConnections[conn.clientID]) == 0 {
				delete(clientConnections, conn.clientID)
			}
			fmt.Println("Removed Conn: ", clientConnections)
			mutex.Unlock()
			return
		}
		//declare message that will hold client message data
		var message msg
		//unmarshal (convert bytes to msg struct)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			fmt.Println("unmarshal error")
			fmt.Println(err)
		}

		//WEBSOCKET MANAGEMENT
		//Initialize all websocket parameters
		if !connected {
			//update conn with new parameters
			//add clientID to connection
			conn.clientID = message.ClientID

			//check if client is already connected on another websocket
			//if client has not been connected, create new connection map
			mutex.Lock()
			if _, contains := clientConnections[message.ClientID]; !contains {
				clientConnections[message.ClientID] = make(map[*ConnWithParameters]struct{})
			}
			mutex.Unlock()
			//add conn to map
			mutex.Lock()
			clientConnections[message.ClientID][conn] = struct{}{}
			fmt.Println("Added Conn", clientConnections)
			mutex.Unlock()
			fmt.Println()
			//update connected to true
			connected = true

			//CHECK COUCHBASE
			//check if client exists in couchbase
			if cbConn.ClientExists(message.ClientID) {
				//query couchbase for client's events
				clientEvents := cbConn.Doc.Events

				//add filters to connection
				for _, event := range clientEvents {
					//add live filters
					conn.filter[event] = struct{}{}
					// Add event to allfilters map
					conn.allFilters[event] = struct{}{}
				}
				//send event options to client
				conn.ws.WriteJSON(clientEvents)
			} else {
				//if clientID does not exist in couchbase
				conn.ws.WriteMessage(1, []byte("Couchbase Error: ClientID not found"))
			}
			//start writing Function
			go intervalFlush(conn)
			//continue to next for loop iteration, skipping updating filters
			continue
		}

		//UPDATE FILTERS
		conn.filter = make(map[string]struct{}) //reinitialize filters because receiving whole array of filters from client
		//iterate through client message filter array and add the elements to the connection filter slice
		for _, event := range message.Filter {
			conn.filter[event] = struct{}{}
		}
	}
}
func intervalFlush(conn *ConnWithParameters) {
	var flushTime time.Time
	for {
		if time.Now().Sub(flushTime) >= batchInterval {
			if len(conn.batchArray) > minBatchSize {
				// fmt.Println("Interval Flush")
				mutex.Lock()
				flush(conn)
				mutex.Unlock()
				flushTime = time.Now()
			}
		}
	}
}

func flush(conn *ConnWithParameters) {
	batch, err := json.Marshal(conn.batchArray)
	if err != nil {
		fmt.Println("batch marshal error")
	}
	conn.ws.WriteJSON(string(batch))
	conn.batchArray = []KafkaData{}
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
	Loop:
		for {
			select {
			// In case of error
			case err := <-consumer.Errors():
				fmt.Println(err)
			// Print consumer messages
			case msg := <-consumer.Messages():
				// fmt.Println("Got data ", string(msg.Value))
				var kafkaData KafkaData
				json.Unmarshal(msg.Value, &kafkaData)

				// Check if ClientID exists
				mutex.Lock()
				if _, contains := clientConnections[kafkaData.ClientID]; contains {
					// If client is connected, get map of connections
					clientConnections := clientConnections[kafkaData.ClientID]
					//iterate over client connections
					for conn := range clientConnections {
						// Check if consume message has a different filter than allfilters
						if _, contains := conn.allFilters[kafkaData.EventID]; !contains {
							fmt.Println("Updating filter")
							updateFilter(conn)
						}

						//if connection filter has KafkaData eventID, send data
						if _, hasEvent := conn.filter[kafkaData.EventID]; hasEvent {
							//check if batchArray is full, if so, flush

							if len(conn.batchArray) == maxBatchSize {
								// fmt.Println("Size Flush")
								flush(conn)
							}
							//add KafkaData of just eventID, lat, lng to batchArray
							// conn.batchArray = append(conn.batchArray, kafkaData)
							conn.batchArray = append(conn.batchArray, KafkaData{
								EventID:   kafkaData.EventID,
								Latitude:  kafkaData.Latitude,
								Longitude: kafkaData.Longitude,
							})
						}
					}
				}
				mutex.Unlock()
			// Service interruption
			case <-signals:
				fmt.Println("Interrupt detected")
				doneCh <- struct{}{}
				break Loop
			}
		}
	}()

	// If everything is done, close consumer
	<-doneCh
	fmt.Println("Consumption closed")
}

// Update filter once there is a change
func updateFilter(conn *ConnWithParameters) {
	// Empty map
	conn.allFilters = make(map[string]struct{})
	// query couchbase for client's events
	clientEvents := cbConn.Doc.Events
	// Iterate through all events in couchbase
	for _, event := range clientEvents {
		conn.allFilters[event] = struct{}{}
	}
	err := conn.ws.WriteJSON(clientEvents)
	if err != nil {
		fmt.Println(err)
	}
}
