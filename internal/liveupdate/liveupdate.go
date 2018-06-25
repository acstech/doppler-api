package liveupdate

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

//GLOBAL VARIABLES
var cbConn *couchbase.Couchbase                                   //used to hold couchbase connection
var clientConnections map[string]map[*ConnWithParameters]struct{} //map used as connection hub, keeps up with clients and their respective connections and each connections settings
var mutex = &sync.RWMutex{}                                       //mutex used for concurrent reading and writing
var count int64 = 5                                               //hard coded weight
var maxBatchSize = 50                                             //max size of data batch that is sent
var minBatchSize = 1                                              //min size of data batch that is sent
var batchInterval = time.Duration(1000 * time.Millisecond)        //millisecond interval that data is sent

//ConnWithParameters is used to add parameters to a gorilla's websocket.Conn
type ConnWithParameters struct {
	ws         *websocket.Conn     //keeps up with connection identifier
	clientID   string              //the clientID associated with this connection
	filter     map[string]struct{} //a map of the events that the client currently wants to see
	allFilters map[string]struct{} //a map of all the events that the client has available
	batchArray []KafkaData         //array used to hold data for batch sending
}

//BatchStruct is the JSON format for batch
type BatchStruct struct {
	BatchArray []KafkaData `json:"batchArray"`
}

//msg is the JSON format messages from client
type msg struct {
	ClientID string   `json:"clientID,omitempty"`
	Filter   []string `json:"Filter,omitempty"`
	//startTime <type> `json:"startTime, omitempty"`
	//endTime <type> `json:"endTime, omitempty"`
}

//KafkaData is the JSON format messages from Kafka
type KafkaData struct {
	Latitude  string `json:"lat,omitempty"`
	Longitude string `json:"lng,omitempty"`
	Count     string `json:"count,omitempty"`
	ClientID  string `json:"clientID,omitempty"`
	EventID   string `json:"eventID,omitempty"`
}

//InitWebsockets initializes websocket requests
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

//readWS continually reads messages from a ConnWithParameters' websocket, initializes connection parameters and updates live filters when necessary
func readWS(conn *ConnWithParameters) {
	defer conn.ws.Close() //close the connection whenever readWS returns

	//boolean used to keep up if this websocket has been connected
	connected := false

	// Continuously read messages that are received
	for {
		//read messages from client
		_, msgBytes, err := conn.ws.ReadMessage()
		//check if client closed connection
		if err != nil {
			//if client closed connection, remove connetion from clientConnections
			closeConnection(conn)
			return //returns out of readWS
		}

		//declare message that will hold client message data
		var message msg
		//unmarshal (convert bytes to msg struct)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			fmt.Println("unmarshal error")
			fmt.Println(err)
		}

		//WEBSOCKET MANAGEMENT
		//If havent been connected, initialize all connection parameters, first message has to be clientID
		if !connected {
			conn = initConn(conn, message)
			//update connected to true
			connected = true
			//continue to next for loop iteration, skipping updating filters
			continue
		}

		//UPDATE LIVE FILTERS - if client has already been connected, only other messages should be filter updates
		conn = updateLiveFilters(conn, message)
	}
}

// Consume consumes messages from queue
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
		if closeErr := master.Close(); err != nil {
			panic(closeErr)
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
	//go func that continually consumes messages from Kafka
	go func() {
	Loop:
		for {
			select {
			// In case of error
			case err := <-consumer.Errors():
				fmt.Println(err)
			// Print consumer messages
			case msg := <-consumer.Messages():
				//initialize variable to hold data from kafka data
				var kafkaData KafkaData
				json.Unmarshal(msg.Value, &kafkaData) //unmarshal data to json

				// Check if ClientID exists
				mutex.Lock()
				if _, contains := clientConnections[kafkaData.ClientID]; contains {
					// If client is connected, get map of connections
					clientConnections := clientConnections[kafkaData.ClientID]
					//iterate over client connections
					for conn := range clientConnections {
						// Check if consume message has a different filter than allfilters
						if _, contains := conn.allFilters[kafkaData.EventID]; !contains {
							updateAvailableFilters(conn, kafkaData.EventID)
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
								// EventID:   kafkaData.EventID,
								Latitude:  kafkaData.Latitude,
								Longitude: kafkaData.Longitude,
								Count:     strconv.FormatInt(count, 10),
							})
						}
					}
				}
				mutex.Unlock()
			// Service interruption
			case <-signals:
				fmt.Println("Interrupt detected")
				fmt.Println(count)
				doneCh <- struct{}{}
				break Loop
			}
		}
	}()

	// If everything is done, close consumer
	<-doneCh
	fmt.Println("Consumption closed")
}

//initConn initialize a ConnWithParameters' parameters based on the first message sent over the websocket
func initConn(conn *ConnWithParameters, message msg) *ConnWithParameters {
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

	//add conn to clients' map of connections
	mutex.Lock()
	clientConnections[message.ClientID][conn] = struct{}{}
	fmt.Println("Added Conn", clientConnections)
	mutex.Unlock()
	fmt.Println()

	//CHECK COUCHBASE for client's data
	//check if client exists in couchbase
	if cbConn.ClientExists(message.ClientID) {
		//query couchbase for client's events
		clientEvents := cbConn.Doc.Events

		//add filters to connection
		for _, event := range clientEvents {
			//add live filters (because defaulting initialize live filters to all)
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
	//start checking if need to flush batch
	go intervalFlush(conn)

	return conn
}

//closeConnection removes the connection from the client, if the client has no connections, removes the client
func closeConnection(conn *ConnWithParameters) {
	fmt.Println("Connection Closed by Client")
	//REMOVE FROM MAP
	mutex.Lock()
	delete(clientConnections[conn.clientID], conn) //delete specific connection
	//check if client has any remaining connections, if so, delete client
	if len(clientConnections[conn.clientID]) == 0 {
		delete(clientConnections, conn.clientID)
	}
	fmt.Println("Removed Conn: ", clientConnections)
	mutex.Unlock()
}

//intervalFlush determines when to flush the batch based on the time of the last flush
func intervalFlush(conn *ConnWithParameters) {
	//initialize time of flush
	var flushTime time.Time
	//continuously check if need to flush because of time interval
	for {
		//see if current time minus last flush time is greater than or equal to the set interval
		//sub returns type Duration, batchInterval is of type Duration
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

//flush marshals the batch to json, sends the batch over the conn's websocket, and emptys the batch
func flush(conn *ConnWithParameters) {
	batch, marshalErr := json.Marshal(conn.batchArray) //marshal to type BatchStruct
	if marshalErr != nil {
		fmt.Println("batch marshal error")
	}
	writeErr := conn.ws.WriteJSON(string(batch)) //send batch to client
	if writeErr != nil {
		fmt.Println(writeErr)
	}
	conn.batchArray = []KafkaData{} //empty batch
}

//updateLiveFilters removes the current filters and sets filter equal to the new filters found in the message
func updateLiveFilters(conn *ConnWithParameters, message msg) *ConnWithParameters {
	conn.filter = make(map[string]struct{}) //empty current filters
	//iterate through client message filter array and add the elements to the connection filter slice
	for _, event := range message.Filter {
		conn.filter[event] = struct{}{} //add the new filters
	}
	return conn
}

// updateAvailableFilters adds a new filter found in consume messages to allFilters and sends the available filters to the client
func updateAvailableFilters(conn *ConnWithParameters, newFilter string) {
	// Add new filter to map
	conn.allFilters[newFilter] = struct{}{}
	//initilize slice for sending to client
	var clientEvents []string
	//add all of the available filters to the slice
	for key := range conn.allFilters {
		clientEvents = append(clientEvents, key)
	}
	//send slice to client
	err := conn.ws.WriteJSON(clientEvents)
	if err != nil {
		fmt.Println(err)
	}
}
