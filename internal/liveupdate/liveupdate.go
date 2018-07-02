package liveupdate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/couchbase/gocb"
	"github.com/gorilla/websocket"
)

//upgrader var used to set parameters for websocket connections
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

//GLOBAL VARIABLES
//general variables
var cbConn *couchbase.Couchbase                                   //used to hold couchbase connection
var clientConnections map[string]map[*ConnWithParameters]struct{} //map used as connection hub, keeps up with clients and their respective connections and each connections settings
var mutex = &sync.RWMutex{}                                       //mutex used for concurrent reading and writing

//batching variables
var maxBatchSize = 50                                      //max size of data batch that is sent
var minBatchSize = 1                                       //min size of data batch that is sent
var batchInterval = time.Duration(1000 * time.Millisecond) //millisecond interval that data is sent

//error variables
var connErr clientError

//bucketing variables
var truncateSize = 1 //determine the number of decimal places we truncate our points to
var zeroTest string  //variable used to handle edge case of "-0", used to compare to edge cases in determining if need the negative sign or not

// clientError will be the error message that is sent to the frontend if any occurr
type clientError struct {
	Error string `json:"Error"`
}

//ConnWithParameters is used to add parameters to a gorilla's websocket.Conn
type ConnWithParameters struct {
	ws         *websocket.Conn     //keeps up with connection identifier
	clientID   string              //the clientID associated with this connection
	filter     map[string]struct{} //a map of the events that the client currently wants to see
	allFilters map[string]struct{} //a map of all the events that the client has available
	batchMap   map[string]Latlng   //map that holds buckets of points
}

//BatchStruct is the JSON format for batch, used to marshal the bucketMap
type BatchStruct struct {
	BatchMap map[string]string `json:"batchMap"`
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
	ClientID  string `json:"clientID,omitempty"`
	EventID   string `json:"eventID,omitempty"`
}

//Latlng is the struct to hold data that is sent to the frontend
type Latlng struct {
	Coords Point `json:"latlng,omitempty"`
	Count  int   `json:"count,omitempty"`
}

// Point that holds the specific lat lng data
type Point struct {
	Latitude  string `json:"latitude,omitempty"`
	Longitude string `json:"longitude,omitempty"`
}

//InitWebsockets initializes websocket request handling
func InitWebsockets(cbConnection string) {
	cbConn = &couchbase.Couchbase{}
	err := cbConn.ConnectToCB(cbConnection)
	if err != nil {
		panic(fmt.Errorf("error connecting to couchbase: %v", err))
	}
	fmt.Println("Connected to Couchbase")
	fmt.Println()

	//intialize connection management
	clientConnections = make(map[string]map[*ConnWithParameters]struct{})
	fmt.Println("Ready to Receive Websocket Requests")
	fmt.Println()

	//initlize zero test for bucketing
	createZeroTest()

	//handle any websocket requests
	http.HandleFunc("/receive/ws", createWS)

	//listen for calls to server
	if err := http.ListenAndServe(":8000", nil); err != nil {
		panic(fmt.Errorf("error setting up the websocket endpoint: %v", err))
	}
	connErr = clientError{}
}

// createWS takes in a TCP request and upgrades that request to a websocket, it also initializes parameters for the connection
func createWS(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP server connection to the WebSocket protocol
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		ws.Close() //close the connection just in case
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - error upgrading connection"))
		return
	}
	fmt.Println("NEW CONNECTION: Connection Upgraded, waiting for ClientID")

	//Initialize conn with parameters
	conn := &ConnWithParameters{
		ws:         ws,
		clientID:   "",
		filter:     make(map[string]struct{}),
		allFilters: make(map[string]struct{}),
		batchMap:   make(map[string]Latlng),
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
		//declare boolean that will determine if connection intialization was successful
		var success bool
		//unmarshal (convert bytes to msg struct)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			connErr.Error = "401: Invalid input"
			err = conn.ws.WriteJSON(connErr)
			if err != nil {
				fmt.Println(err)
			}
		}

		//WEBSOCKET MANAGEMENT
		//If havent been connected, initialize all connection parameters, first message has to be clientID
		if !connected {
			conn, success = initConn(conn, message) //initilaize the connection with parameters, return the intilailized connection and if the initialization was succesful
			if success {
				//update connected to true
				connected = true
			}
			//continue to next for loop iteration, skipping updating filters
			continue
		}

		//UPDATE LIVE FILTERS - if client has already been connected, only other messages should be filter updates
		conn = updateLiveFilters(conn, message)
	}
}

// Consume consumes messages from queue
func Consume() error {
	fmt.Println("Kafka Consume Started")
	// Create a new configuration instance
	config := sarama.NewConfig()
	// Specify brokers address. 9092 is default
	brokers := []string{"localhost:9092"}

	// Create a new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return err
	}

	// Wait to close after everything is processed
	defer func() {
		err = master.Close()
	}()

	// Topic to consume
	topic := "influx-topic"

	// ConsumePartition creates a PartitionConsumer on the given topic/partition with the given offset
	// A PartitionConsumer processes messages from a given topic and partition
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return err
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
			case err = <-consumer.Errors():
				fmt.Println(err)
			// Print consumer messages
			case msg := <-consumer.Messages():
				//initialize variable to hold data from kafka data
				var kafkaData KafkaData
				err = json.Unmarshal(msg.Value, &kafkaData) //unmarshal data to json
				if err != nil {
					fmt.Println(err)
				}
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

							if len(conn.batchMap) == maxBatchSize {
								flush(conn)
							}
							//add KafkaData of just eventID, lat, lng to batchArray
							bucketPoints(conn, Latlng{
								// EventID:   kafkaData.EventID,
								Coords: Point{
									Latitude:  kafkaData.Latitude,
									Longitude: kafkaData.Longitude,
								},
								Count: 1,
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
	return err
}

//initConn initialize a ConnWithParameters' parameters based on the first message sent over the websocket
func initConn(conn *ConnWithParameters, message msg) (*ConnWithParameters, bool) {
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
	exists, document, err := cbConn.ClientExists(message.ClientID)
	if err != nil {
		if err == gocb.ErrTimeout {
			connErr.Error = "501: Unable to validate clientID"
			err = conn.ws.WriteJSON(connErr)
			if err != nil {
				fmt.Println(err)
			}
		} else if err == gocb.ErrBusy {
			connErr.Error = "502: Unable to validate clientID"
			err = conn.ws.WriteJSON(connErr)
			if err != nil {
				fmt.Println(err)
			}
		} else {
			connErr.Error = "503: Unable to validate clientID"
			err = conn.ws.WriteJSON(connErr)
			if err != nil {
				fmt.Println(err)
			}
		}
		closeConnection(conn)
		return conn, false
	}
	if exists {
		//query couchbase for client's events
		clientEvents := document.Events

		//add filters to connection
		for _, event := range clientEvents {
			//add live filters (because defaulting initialize live filters to all)
			conn.filter[event] = struct{}{}
			// Add event to allfilters map
			conn.allFilters[event] = struct{}{}
		}
		//send event options to client
		err = conn.ws.WriteJSON(clientEvents)
		if err != nil {
			fmt.Println(err)
		}
	} else {
		//if clientID does not exist in couchbase
		connErr.Error = "401: The ClientID is not valid"
		err = conn.ws.WriteJSON(connErr)
		if err != nil {
			fmt.Println(err)
		}
		closeConnection(conn)
		return conn, false
	}
	//start checking if need to flush batch but iff no other checking has started because
	// this takes a lot of CPU
	if len(clientConnections) == 1 {
		go intervalFlush()
	}

	return conn, true
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
func intervalFlush() {
	//initialize time of flush
	var flushTime time.Time
	//continuously check if need to flush because of time interval
	for {
		// check to see if any clients are connected
		if len(clientConnections) == 0 { // no clients are connected, so free up the CPU
			return
		}
		mutex.Lock()                              // make sure that nothing writes to the map while it is being looked at
		for _, conns := range clientConnections { // get each set connections
			for conn := range conns { // check to see if each connection needs to be flushed
				//see if current time minus last flush time is greater than or equal to the set interval
				//sub returns type Duration, batchInterval is of type Duration
				if time.Now().Sub(flushTime) >= batchInterval {
					if len(conn.batchMap) >= minBatchSize {
						flush(conn)
						flushTime = time.Now()
					}
				}
			}
		}
		mutex.Unlock()
	}
}

//flush marshals the batch to json, sends the batch over the conn's websocket, and emptys the batch
func flush(conn *ConnWithParameters) {
	batch, marshalErr := json.Marshal(conn.batchMap) //marshal to type BatchStruct
	if marshalErr != nil {
		fmt.Println("batch marshal error")
		fmt.Println(marshalErr)
	}
	writeErr := conn.ws.WriteJSON(string(batch)) //send batch to client
	if writeErr != nil {
		fmt.Println("Flush Write Error")
		fmt.Println(writeErr)
	}
	conn.batchMap = make(map[string]Latlng) //empty batch
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

// bucketPoints takes a connection and a point and puts them in the batch of buckets as necessary
func bucketPoints(conn *ConnWithParameters, rawPt Latlng) {
	// Truncate each item in batch
	// Split float by decimal
	latSlice := strings.SplitAfter(rawPt.Coords.Latitude, ".")
	lngSlice := strings.SplitAfter(rawPt.Coords.Longitude, ".")

	// Truncate second half of slices
	latSlice[1] = truncate(latSlice[1])
	lngSlice[1] = truncate(lngSlice[1])

	//check for truncating edge case
	if strings.Contains(latSlice[0], "-0.") {
		latSlice = checkZero(latSlice)
	}
	if strings.Contains(lngSlice[0], "-0.") {
		lngSlice = checkZero(lngSlice)
	}

	// Combine the split strings together
	lat := strings.Join(latSlice, "")
	lng := strings.Join(lngSlice, "")

	//create bucket hash
	bucket := lat + ":" + lng

	//create point
	pt := Latlng{
		Coords: Point{
			Latitude:  lat,
			Longitude: lng,
		},
		Count: 1,
	}

	// Bucketing
	// check if bucket exists
	// if it does exists, increase the count
	if _, contains := conn.batchMap[bucket]; contains {
		value := conn.batchMap[bucket] //get the value of the bucket
		value.Count++                  //increase the count
		conn.batchMap[bucket] = value  //add the new count to the point
	} else { //otherwise, add the point with the count
		conn.batchMap[bucket] = pt
	}
}

// trucate takes a string and changes its length based on truncateSize
func truncate(s string) string {
	if len(s) < truncateSize {
		//padding if smaller
		for i := len(s); i < truncateSize; i++ {
			s += "0"
		}
		return s
	}
	//truncate
	return s[0:truncateSize]
}

// createZeroTest creates the zeroTest variable based on the truncateSize, which is used to handle "-0." edge case
func createZeroTest() {
	// loops based on how much we are truncating
	for i := 0; i < truncateSize; i++ {
		zeroTest = zeroTest + "0" //append zeros
	}
}

// checkZero determines if a "-0." edge case needs to remove the "-" and does so if necessary
func checkZero(coord []string) []string {
	//compare the decimals of the "-0." case to the zeroTest
	//if they are equal, remove the "-"
	if strings.Compare(coord[1], zeroTest) == 0 {
		coord[0] = "0."
		return coord
	}
	return coord
}
