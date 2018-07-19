package service

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/acstech/doppler-api/internal/couchbase"
	"github.com/couchbase/gocb"
	"github.com/gorilla/websocket"
)

// ConnectionManager defines the parameters of handling connections for doppler
type ConnectionManager struct {
	connections          map[string]map[*ConnWithParameters]struct{} // map used as connection hub, keeps up with clients and their respective connections and each connections settings
	upgrader             websocket.Upgrader                          // upgrader defines the parameters of the websockets
	maxBatchSize         int                                         // int that represents the maximum size of a batch that is sent to a connection, batch is automatically sent when this size is reached
	minBatchSize         int                                         // int that represents the minimum size of a batch that is sent to a connection, ensures that intervalFlush does send an emoty batch
	batchInterval        time.Duration                               //duration that represents the amount of time between each intervalFlush
	defaultTruncateSize  int                                         // int that represents the default truncation size of all points in connectionManager, used in bucketPoints
	intervalFlushStarted bool                                        // bool that keeps up with if interval flushing has started
	cbConn               *couchbase.Couchbase                        // used to hold couchbase connection
	mutex                sync.RWMutex                                // mutex used for concurrent reading and writing
}

// ConnWithParameters is used to add parameters to a gorilla's websocket.Conn
type ConnWithParameters struct {
	connectionManager *ConnectionManager  // the connectionManager that this connection is managed by
	ws                *websocket.Conn     // keeps up with connection identifier
	clientID          string              // the clientID associated with this connection
	activeFilters     map[string]struct{} // a map of the events that the client currently wants to see
	allFilters        map[string]struct{} // a map of all the events that the client has available
	batchMap          map[string]Latlng   // map that holds buckets (truncated points) of points with count
	flushTime         time.Time           // time that represents the time of the last flush
	truncateSize      int                 // int that represents the truncation size of a specific point, used in bucketPoints
	zeroTest          string              // string used to compare to handle truncation edge case
	ConnErr           ConnErr             // ConnErr used to hold errors that are sent to connections
	mutex             sync.RWMutex        // mutex used for concurrent reading and writing
}

// msg is the JSON format messages from client
type msg struct {
	ClientID string   `json:"clientID,omitempty"` // string that holds ClientID received from a websocket
	Filter   []string `json:"Filter,omitempty"`   // []string that holds the current active filters received from a websocket
	//startTime <type> `json:"startTime, omitempty"`
	//endTime <type> `json:"endTime, omitempty"`
}

// ConnErr is the struct for error messages sent over websocket
type ConnErr struct {
	Err string `json:"Error"`
}

// NewConnectionManager initializes the connectionManager
// requires maxBatchSize, minBatchSize, batchInterval (in milliseconds), truncateSize, cbConn
func NewConnectionManager(maxBS int, minBS int, batchMilli int, tSize int, cbConnection *couchbase.Couchbase) *ConnectionManager {
	// initialize connections
	connections := make(map[string]map[*ConnWithParameters]struct{})

	// intialize upgrader
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	// create batch interval based on milliseconds that were passed in
	bInterval := time.Duration(time.Duration(batchMilli) * time.Millisecond)

	log.Println("Ready to Receive Websocket Requests")

	// return a ConnectionManager with all parameters
	return &ConnectionManager{
		connections:          connections,
		upgrader:             upgrader,
		maxBatchSize:         maxBS,
		minBatchSize:         minBS,
		batchInterval:        bInterval,
		defaultTruncateSize:  tSize,
		intervalFlushStarted: false,
		cbConn:               cbConnection,
		mutex:                sync.RWMutex{},
	}
}

// ServeHTTP takes in a TCP request, upgrades that request to a websocket, and intializes the connections' parameters
func (c *ConnectionManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP server connection to the WebSocket protocol
	c.mutex.RLock()
	ws, err := c.upgrader.Upgrade(w, r, nil)
	c.mutex.RUnlock()
	if err != nil {
		ws.Close() //close the connection just in case
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - error upgrading connection"))
		return
	}

	log.Println("NEW CONNECTION: Connection Upgraded, waiting for ClientID")

	// Initialize conn with parameters
	conn := &ConnWithParameters{
		connectionManager: c,
		ws:                ws,
		clientID:          "",
		activeFilters:     make(map[string]struct{}),
		allFilters:        make(map[string]struct{}),
		batchMap:          make(map[string]Latlng),
		flushTime:         time.Now(),
		truncateSize:      c.defaultTruncateSize,
		zeroTest:          "",
		ConnErr:           ConnErr{},
		mutex:             sync.RWMutex{},
	}
	// now listen for messages for this created websocket
	go conn.readWS()
}

// readWS continually reads messages from a ConnWithParameters' websocket, initializes connection parameters and updates live filters when necessary
func (conn *ConnWithParameters) readWS() {
	defer conn.ws.Close() // close the connection whenever readWS returns
	// boolean used to keep up if this websocket has been connected
	connected := false

	// Continuously read messages that are received
	for {
		// read messages from client
		_, msgBytes, err := conn.ws.ReadMessage()
		// check if client closed connection
		if err != nil {
			// check if the client was registered
			if connected {
				// if client closed connection, remove connetion from clientConnections
				conn.connectionManager.unregisterConn(conn)
			}
			return // returns out of readWS
		}

		// declare message that will hold client message data
		var message msg
		// declare boolean that will determine if connection intialization was successful
		var success bool
		// unmarshal (convert bytes to msg struct)
		if err := json.Unmarshal(msgBytes, &message); err != nil {
			conn.mutex.RLock()
			conn.ConnErr.Err = "401: Invalid input"
			err = conn.ws.WriteJSON(conn.ConnErr)
			conn.mutex.RUnlock()
			if err != nil {
				log.Println("readWS unmarshal msg error ", err)
			}
		}
		// If havent been connected, initialize all connection parameters, first message has to be clientID
		if !connected {
			success = conn.connectionManager.registerConn(conn, message) //initilaize the connection with parameters, return the intilailized connection and if the initialization was successful
			if success {
				// update connected to true
				connected = true
				// start checking if need to flush batch but iff no other checking has started
				if len(conn.connectionManager.connections) == 1 && conn.connectionManager.intervalFlushStarted == false {
					conn.connectionManager.intervalFlushStarted = true // update connection manager interval flush start
					go conn.connectionManager.intervalFlush()          // start interval flushing
				}

			} else {
				// if regisitering was not successful, remove the connection from the connection manager
				conn.connectionManager.unregisterConn(conn)
			}
			// continue to next for loop iteration, skipping updating filters on first iteration
			continue
		}

		// if client has already been connected, only other messages should be filter updates
		conn.updateActiveFilters(message.Filter)
	}
}

// registerConn updates a ConnWithParameters' parameters based on the first message sent over the websocket and adds it adds the connection to the connectionManager's connnections
func (c *ConnectionManager) registerConn(conn *ConnWithParameters, message msg) bool {
	// write lock
	c.mutex.Lock()
	conn.mutex.Lock()

	// unlock on return
	defer func() {
		c.mutex.Unlock()
		conn.mutex.Unlock()
	}()

	// update conn with new parameters
	// add clientID to connection
	conn.clientID = message.ClientID

	// check if client is already connected on another websocket
	// if client has not been connected, create new connection map
	if _, contains := c.connections[conn.clientID]; !contains {
		c.connections[conn.clientID] = make(map[*ConnWithParameters]struct{})
	}
	// add conn to clients' map of connections
	c.connections[conn.clientID][conn] = struct{}{}
	log.Println("Added Conn", c.connections)

	// CHECK COUCHBASE for client's data
	// check if client exists in couchbase
	exists, document, err := c.cbConn.ClientExists(conn.clientID)
	if err != nil {
		if err == gocb.ErrTimeout {
			conn.ConnErr.Err = "501: Unable to validate clientID"
			err = conn.ws.WriteJSON(conn.ConnErr)
			if err != nil {
				log.Println(err)
			}
		} else if err == gocb.ErrBusy {
			conn.ConnErr.Err = "502: Unable to validate clientID"
			err = conn.ws.WriteJSON(conn.ConnErr)
			if err != nil {
				log.Println(err)
			}
		} else {
			conn.ConnErr.Err = "503: Unable to validate clientID"
			err = conn.ws.WriteJSON(conn.ConnErr)
			if err != nil {
				log.Println(err)
			}
		}
		return false // registering not successful
	}
	// if client exists in Couchbase
	if exists {
		// query couchbase for client's events
		clientEvents := document.Events

		// add filters to connection' activeFilters and allFilters
		for _, event := range clientEvents {
			conn.activeFilters[event] = struct{}{} // add live filters (because defaulting initialize live filters to all)
			conn.allFilters[event] = struct{}{}    // add event to allfilters map
		}

		// send event options to client
		err = conn.ws.WriteJSON(clientEvents)
		if err != nil {
			log.Println(err)
		}
	} else {
		// if clientID does not exist in couchbase
		conn.ConnErr.Err = "401: The ClientID is not valid"
		err = conn.ws.WriteJSON(conn.ConnErr)
		if err != nil {
			log.Println(err)
		}

		return false // registering not successful
	}

	// initlize zero test for bucketing based on connection manager default truncation size
	conn.zeroTest = createZeroTest(conn.connectionManager.defaultTruncateSize)

	return true // register successful
}

// unregisterConn removes the connection from the client, if the client has no connections, removes the client
func (c *ConnectionManager) unregisterConn(conn *ConnWithParameters) {
	c.mutex.Lock()     // write lock
	conn.mutex.RLock() // read lock

	// unlock on return
	defer func() {
		c.mutex.Unlock()
		conn.mutex.RUnlock()
	}()

	log.Println("Connection Closed by Client")
	// REMOVE FROM MAP
	delete(c.connections[conn.clientID], conn) // delete specific connection

	// check if client has any remaining connections, if so, delete client
	if len(c.connections[conn.clientID]) == 0 {
		delete(c.connections, conn.clientID)
	}
	log.Println("Removed Conn: ", c.connections)
}

// updateActiveFilters removes the current filters and sets filter equal to the new filters found in the message
func (conn *ConnWithParameters) updateActiveFilters(newFilters []string) {
	conn.mutex.Lock()         // write lock
	defer conn.mutex.Unlock() // unlock on return

	conn.activeFilters = make(map[string]struct{}) // empty current filters
	// iterate through client message filter array and add the elements to the connection filter slice
	for _, event := range newFilters {
		conn.activeFilters[event] = struct{}{} // add the new filters
	}
}

// intervalFlush determines when to flush the batch based on the time of the last flush
func (c *ConnectionManager) intervalFlush() {
	// continuously check if need to flush because of time interval
	for {
		c.mutex.RLock() // read lock
		// check to see if any clients are connected
		if len(c.connections) == 0 { // no clients are connected, so free up the CPU
			c.intervalFlushStarted = false
			c.mutex.RUnlock() // unlock on return
			return
		}

		for _, clientIDs := range c.connections { // get each clientID
			for conn := range clientIDs { // check to see if each connection needs to be flushed
				// see if current time minus last flush time is greater than or equal to the set interval
				// sub returns type Duration, batchInterval is of type Duration
				conn.mutex.Lock()
				if time.Now().Sub(conn.flushTime) >= c.batchInterval {
					if len(conn.batchMap) >= c.minBatchSize {
						conn.flush()
						conn.flushTime = time.Now()
					}
				}
				conn.mutex.Unlock() // unlock
			}
		}
		c.mutex.RUnlock() //unlock
	}
}

// flush marshals the batch to json, sends the batch over the conn's websocket, and emptys the batch
func (conn *ConnWithParameters) flush() {
	batch, marshalErr := json.Marshal(conn.batchMap) // marshal to type BatchStruct
	if marshalErr != nil {
		log.Println("batch marshal error", marshalErr)
	}

	writeErr := conn.ws.WriteJSON(string(batch)) // send batch to client
	if writeErr != nil {
		log.Println("Connection Write Batch Error: ", writeErr)
	}
	conn.batchMap = make(map[string]Latlng) // empty batch
}

// createZeroTest creates the zeroTest variable based on the truncateSize, which is used to handle "-0." edge case
func createZeroTest(truncateSize int) string {
	// loops based on how much we are truncating
	var zeroTest string
	for i := 0; i < truncateSize; i++ {
		zeroTest = zeroTest + "0" // append zeros
	}
	return zeroTest
}
