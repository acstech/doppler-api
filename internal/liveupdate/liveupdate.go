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

//upgrader var used to set parameters for websocket connections
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var cbConn *couchbase.Couchbase                                   //used to hold couchbase connection
var clientConnections map[string]map[*ConnWithParameters]struct{} //map used as connection hub, keeps up with clients and their respective connections and each connections settings
var mutex = &sync.Mutex{}                                         //mutex used for concurrent reading and writing
var count int = 0                                                 //hard coded weight

//used to add parameters to a gorilla's websocket.Conn
type ConnWithParameters struct {
	ws       *websocket.Conn     //keeps up with connection identifier
	clientID string              //the clientID associated with this connection
	filter   map[string]struct{} //a map of the events that the client currently wants to see
	// allFilters map[string]struct{} //a map of all the events that the client has available
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
	Latitude  string `json:"lat"`
	Longitude string `json:"lng"`
	Count     string `json:"count"`
	ClientID  string `json:"clientID"`
	EventID   string `json:"eventID"`
	// Insert time eventually
}

// InitWebsockets starts listening for websocket requests and returns an error if any occur
func InitWebsockets(cbConnection string) {
	//create CB connection
	cbConn = &couchbase.Couchbase{Doc: &couchbase.Doc{}}
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

	//handle any websocket requests
	http.HandleFunc("/receive/ws", createWS)

	//listen for calls to server
	if err := http.ListenAndServe(":8000", nil); err != nil {
		panic(fmt.Errorf("error setting up the websocket endpoint: %v", err))
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
		ws:       ws,
		clientID: "",
		filter:   make(map[string]struct{}),
	}

	errs := make(chan error, 1)
	//now listen for messages for this created websocket
	go readWS(conn, &errs)
}

func readWS(conn *ConnWithParameters, errs *<-chan error) error {
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
			return nil
		}
		//declare message that will hold client message data
		var message msg
		//unmarshal (convert bytes to msg struct)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			return fmt.Errorf("unmarshaling error: %v", err)
		}

		//WEBSOCKET MANAGEMENT
		// if websocket hasnt been added to clientConnections map
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

			//check if client exists in couchbase
			exists, err := cbConn.ClientExists(message.ClientID)
			if err != nil {
				return fmt.Errorf("error connecting to couchbase: %v", err)
			}
			if exists {
				//query couchbase for client's events
				clientEvents := cbConn.Doc.Events
				//add filters to connection
				for _, event := range clientEvents {
					conn.filter[event] = struct{}{}
				}
				//send event options to client
				conn.ws.WriteJSON(clientEvents)
			} else {
				//if clientID does not exist in couchbase
				conn.ws.WriteMessage(1, []byte("Couchbase Error: ClientID not found"))
			}
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

// Consume messages from queue
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
	defer func() error {
		if err := master.Close(); err != nil {
			return err
		}
		return nil
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
					for conn, _ := range clientConnections {
						//TODO Update allFilters if find a new filter and send new filter options to front end
						//if connection filter has KafkaData eventID, send data
						if _, hasEvent := conn.filter[kafkaData.EventID]; hasEvent {
							conn.ws.WriteJSON(kafkaData)
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
	return nil
}
